import { InjectFlowProducer, Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { FlowProducer, Job } from 'bullmq';
import { createWriteStream } from 'node:fs';
import { mkdir, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { PrismaService } from '../../prisma/prisma.service';
import {
  FINALIZE_QUEUE,
  IMAGE_QUEUE,
  INGEST_QUEUE,
  PIPELINE_FLOW_PRODUCER,
  THUMBNAIL_QUEUE,
  TRANSCODE_1080P_QUEUE,
  TRANSCODE_720P_QUEUE,
} from '../../queue/queue.constants';
import { S3Service } from '../../s3/s3.service';
import {
  FinalizeJobData,
  ImageJobData,
  ImageOptions,
  IngestJobData,
  PipelineDestination,
  PipelineSource,
  ThumbnailJobData,
  TranscodeJobData,
} from '../../pipeline/types';

export class IngestResult {
  source: PipelineSource;
}

@Processor(INGEST_QUEUE)
export class IngestProcessor extends WorkerHost {
  private readonly logger = new Logger(IngestProcessor.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly s3: S3Service,
    @InjectFlowProducer(PIPELINE_FLOW_PRODUCER)
    private readonly flow: FlowProducer,
  ) {
    super();
  }

  async process(job: Job<IngestJobData>): Promise<IngestResult> {
    const { taskId, presignedUrl, directUpload, destination } = job.data;
    this.logger.log(`Ingest start task=${taskId}`);

    const task = await this.prisma.mediaTask.update({
      where: { id: taskId },
      data: { status: 'PROCESSING', progress: 1 },
    });

    const metadata = (task.metadata ?? {}) as Record<string, unknown>;
    const originalFilename =
      typeof metadata.originalFilename === 'string'
        ? metadata.originalFilename
        : undefined;

    const source = directUpload
      ? await this.acquireFromDirectUpload({
          taskId,
          destination,
          metadata,
          byteSize: directUpload.byteSize,
          contentType: directUpload.contentType,
          originalFilename,
        })
      : await this.acquireFromPresignedUrl({
          taskId,
          presignedUrl,
          destination,
          metadata,
          originalFilename,
        });

    const finalizeData: FinalizeJobData = {
      taskId,
      destination,
      source,
    };

    if (task.type === 'IMAGE') {
      const imageOptions = (metadata.imageOptions ?? {}) as ImageOptions;
      const imageData: ImageJobData = {
        taskId,
        source,
        destination,
        options: imageOptions,
      };
      await this.flow.add({
        name: 'finalize',
        queueName: FINALIZE_QUEUE,
        data: finalizeData,
        opts: { removeOnComplete: 50, removeOnFail: 200 },
        children: [
          {
            name: 'image',
            queueName: IMAGE_QUEUE,
            data: imageData,
            opts: {
              attempts: 2,
              backoff: { type: 'exponential', delay: 5_000 },
            },
          },
        ],
      });
    } else {
      const transcodeData: TranscodeJobData = {
        taskId,
        source,
        destination,
      };
      const thumbnailData: ThumbnailJobData = transcodeData;

      await this.flow.add({
        name: 'finalize',
        queueName: FINALIZE_QUEUE,
        data: finalizeData,
        opts: { removeOnComplete: 50, removeOnFail: 200 },
        children: [
          {
            name: 'transcode-1080p',
            queueName: TRANSCODE_1080P_QUEUE,
            data: transcodeData,
            opts: {
              attempts: 2,
              backoff: { type: 'exponential', delay: 10_000 },
            },
          },
          {
            name: 'transcode-720p',
            queueName: TRANSCODE_720P_QUEUE,
            data: transcodeData,
            opts: {
              attempts: 2,
              backoff: { type: 'exponential', delay: 10_000 },
            },
          },
          {
            name: 'thumbnail',
            queueName: THUMBNAIL_QUEUE,
            data: thumbnailData,
            opts: {
              attempts: 2,
              backoff: { type: 'exponential', delay: 5_000 },
            },
          },
        ],
      });
    }

    this.logger.log(
      `Ingest done task=${taskId} type=${task.type} bytes=${source.byteSize ?? '?'}`,
    );
    return { source };
  }

  private async acquireFromPresignedUrl(input: {
    taskId: string;
    presignedUrl: string | undefined;
    destination: PipelineDestination;
    metadata: Record<string, unknown>;
    originalFilename: string | undefined;
  }): Promise<PipelineSource> {
    if (!input.presignedUrl) {
      throw new Error(
        `Ingest task=${input.taskId} has no source: presignedUrl and directUpload both missing`,
      );
    }

    const workDir = join(tmpdir(), `media-${input.taskId}`);
    await mkdir(workDir, { recursive: true });
    const localPath = join(workDir, 'source.bin');

    try {
      const response = await fetch(input.presignedUrl);
      if (!response.ok || !response.body) {
        throw new Error(
          `Failed to download source: ${response.status} ${response.statusText}`,
        );
      }
      const contentType =
        response.headers.get('content-type') ?? 'application/octet-stream';

      await pipeline(
        Readable.fromWeb(response.body as never),
        createWriteStream(localPath),
      );

      const { size } = await stat(localPath);
      const sourceKey = `${input.destination.prefix}_source/source.bin`;

      await this.s3.uploadFile({
        key: sourceKey,
        filePath: localPath,
        contentType,
      });

      await this.prisma.mediaTask.update({
        where: { id: input.taskId },
        data: {
          progress: 10,
          sourceKey,
          metadata: {
            ...input.metadata,
            byteSize: size,
            contentType,
          },
        },
      });

      return {
        bucket: input.destination.bucket,
        key: sourceKey,
        contentType,
        byteSize: size,
        originalFilename: input.originalFilename,
      };
    } finally {
      await rm(workDir, { recursive: true, force: true });
    }
  }

  private async acquireFromDirectUpload(input: {
    taskId: string;
    destination: PipelineDestination;
    metadata: Record<string, unknown>;
    byteSize: number;
    contentType: string;
    originalFilename: string | undefined;
  }): Promise<PipelineSource> {
    const sourceKey = `${input.destination.prefix}_source/source.bin`;

    await this.prisma.mediaTask.update({
      where: { id: input.taskId },
      data: {
        progress: 10,
        sourceKey,
        metadata: {
          ...input.metadata,
          byteSize: input.byteSize,
          contentType: input.contentType,
        },
      },
    });

    return {
      bucket: input.destination.bucket,
      key: sourceKey,
      contentType: input.contentType,
      byteSize: input.byteSize,
      originalFilename: input.originalFilename,
    };
  }
}
