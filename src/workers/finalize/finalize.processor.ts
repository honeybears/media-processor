import { InjectQueue, Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job, Queue } from 'bullmq';
import { PrismaService } from '../../prisma/prisma.service';
import {
  FinalizeJobData,
  ImageResult,
  RenditionResult,
  ThumbnailResult,
  WebhookImage,
  WebhookJobData,
  WebhookPayload,
  WebhookRendition,
} from '../../pipeline/types';
import { FINALIZE_QUEUE, WEBHOOK_QUEUE } from '../../queue/queue.constants';
import { S3Service } from '../../s3/s3.service';

type ChildResult = RenditionResult | ThumbnailResult | ImageResult;

function isRendition(value: ChildResult): value is RenditionResult {
  return (value as RenditionResult).rendition !== undefined;
}

function isImage(value: ChildResult): value is ImageResult {
  return (value as ImageResult).type === 'image';
}

export class FinalizeResult {
  masterKey?: string;
  outputKey?: string;
}

@Processor(FINALIZE_QUEUE)
export class FinalizeProcessor extends WorkerHost {
  private readonly logger = new Logger(FinalizeProcessor.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly s3: S3Service,
    @InjectQueue(WEBHOOK_QUEUE) private readonly webhookQueue: Queue,
  ) {
    super();
  }

  async process(job: Job<FinalizeJobData>): Promise<FinalizeResult> {
    const { taskId, destination } = job.data;
    this.logger.log(`Finalize start task=${taskId}`);

    const childrenValues = await job.getChildrenValues();
    const results = Object.values(childrenValues) as ChildResult[];
    const image = results.find(isImage);

    if (image) {
      return this.finalizeImage(taskId, destination, image);
    }

    return this.finalizeVideo(taskId, destination, results);
  }

  private async finalizeImage(
    taskId: string,
    destination: FinalizeJobData['destination'],
    image: ImageResult,
  ): Promise<FinalizeResult> {
    await this.s3.deletePrefix(`${destination.prefix}_source/`);

    const finishedAt = new Date();
    const task = await this.prisma.mediaTask.update({
      where: { id: taskId },
      data: {
        status: 'COMPLETED',
        progress: 100,
        finishedAt,
        metadata: {
          outputKey: image.outputKey,
          format: image.format,
          width: image.width,
          height: image.height,
          byteSize: image.byteSize,
        },
      },
    });

    if (task.callbackUrl) {
      const webhookImage: WebhookImage = {
        outputKey: image.outputKey,
        format: image.format,
        width: image.width,
        height: image.height,
        byteSize: image.byteSize,
      };
      const payload: WebhookPayload = {
        taskId,
        tenantId: task.tenantId,
        status: 'COMPLETED',
        bucket: destination.bucket,
        image: webhookImage,
        finishedAt: finishedAt.toISOString(),
      };
      const data: WebhookJobData = {
        taskId,
        callbackUrl: task.callbackUrl,
        payload,
      };
      await this.webhookQueue.add('deliver', data, {
        attempts: 5,
        backoff: { type: 'exponential', delay: 5_000 },
        removeOnComplete: 100,
        removeOnFail: 500,
      });
    }

    this.logger.log(`Finalize done (image) task=${taskId}`);
    return { outputKey: image.outputKey };
  }

  private async finalizeVideo(
    taskId: string,
    destination: FinalizeJobData['destination'],
    results: ChildResult[],
  ): Promise<FinalizeResult> {
    const renditions = results
      .filter(isRendition)
      .sort((a, b) => b.height - a.height);

    if (renditions.length === 0) {
      throw new Error(`No rendition results for task=${taskId}`);
    }

    const lines: string[] = ['#EXTM3U', '#EXT-X-VERSION:3'];
    for (const r of renditions) {
      lines.push(
        `#EXT-X-STREAM-INF:BANDWIDTH=${r.bandwidth},RESOLUTION=${r.width}x${r.height},CODECS="${r.codecs}"`,
      );
      lines.push(`${r.rendition}/playlist.m3u8`);
    }
    const masterContent = lines.join('\n') + '\n';

    const masterKey = `${destination.prefix}master.m3u8`;
    await this.s3.putObject({
      key: masterKey,
      body: masterContent,
      contentType: 'application/vnd.apple.mpegurl',
    });

    await this.s3.deletePrefix(`${destination.prefix}_source/`);

    const finishedAt = new Date();
    const task = await this.prisma.mediaTask.update({
      where: { id: taskId },
      data: {
        status: 'COMPLETED',
        progress: 100,
        finishedAt,
        metadata: {
          masterKey,
          renditions: renditions.map((r) => ({
            name: r.rendition,
            playlistKey: r.playlistKey,
            width: r.width,
            height: r.height,
            bandwidth: r.bandwidth,
          })),
        },
      },
    });

    if (task.callbackUrl) {
      const webhookRenditions: WebhookRendition[] = renditions.map((r) => ({
        name: r.rendition,
        playlistKey: r.playlistKey,
        width: r.width,
        height: r.height,
        bandwidth: r.bandwidth,
      }));
      const payload: WebhookPayload = {
        taskId,
        tenantId: task.tenantId,
        status: 'COMPLETED',
        bucket: destination.bucket,
        masterKey,
        renditions: webhookRenditions,
        finishedAt: finishedAt.toISOString(),
      };
      const data: WebhookJobData = {
        taskId,
        callbackUrl: task.callbackUrl,
        payload,
      };
      await this.webhookQueue.add('deliver', data, {
        attempts: 5,
        backoff: { type: 'exponential', delay: 5_000 },
        removeOnComplete: 100,
        removeOnFail: 500,
      });
    }

    this.logger.log(`Finalize done (video) task=${taskId}`);
    return { masterKey };
  }
}
