import { InjectQueue } from '@nestjs/bullmq';
import { BadRequestException, Injectable } from '@nestjs/common';
import { MediaTaskStatus, Prisma } from '@prisma/client';
import { Queue } from 'bullmq';
import { unlink } from 'node:fs/promises';
import { PrismaService } from '../prisma/prisma.service';
import { S3Service } from '../s3/s3.service';
import { INGEST_QUEUE } from '../queue/queue.constants';
import { CreateImagePipelineDto } from './dto/create-image-pipeline.dto';
import { CreateVideoPipelineDto } from './dto/create-video-pipeline.dto';
import { UploadImagePipelineDto } from './dto/upload-image-pipeline.dto';
import { UploadVideoPipelineDto } from './dto/upload-video-pipeline.dto';
import { DirectUploadDescriptor, ImageOptions, IngestJobData } from './types';

export class CreatePipelineResult {
  taskId: string;
  status: MediaTaskStatus;
}

type MediaType = 'VIDEO' | 'IMAGE';
type PrefixRoot = 'videos' | 'images';

@Injectable()
export class PipelineService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly s3: S3Service,
    @InjectQueue(INGEST_QUEUE) private readonly ingestQueue: Queue,
  ) {}

  async createVideoPipeline(
    dto: CreateVideoPipelineDto,
  ): Promise<CreatePipelineResult> {
    return this.enqueueRemote({
      type: 'VIDEO',
      destinationPrefixRoot: 'videos',
      presignedUrl: dto.presignedUrl,
      originalFilename: dto.originalFilename,
      tenantId: dto.tenantId,
      callbackUrl: dto.callbackUrl,
    });
  }

  async createImagePipeline(
    dto: CreateImagePipelineDto,
  ): Promise<CreatePipelineResult> {
    return this.enqueueRemote({
      type: 'IMAGE',
      destinationPrefixRoot: 'images',
      presignedUrl: dto.presignedUrl,
      originalFilename: dto.originalFilename,
      tenantId: dto.tenantId,
      callbackUrl: dto.callbackUrl,
      imageOptions: dto.options,
    });
  }

  async uploadVideoPipeline(input: {
    file: Express.Multer.File;
    dto: UploadVideoPipelineDto;
  }): Promise<CreatePipelineResult> {
    return this.enqueueDirect({
      type: 'VIDEO',
      destinationPrefixRoot: 'videos',
      file: input.file,
      originalFilename: input.dto.originalFilename ?? input.file.originalname,
      tenantId: input.dto.tenantId,
      callbackUrl: input.dto.callbackUrl,
    });
  }

  async uploadImagePipeline(input: {
    file: Express.Multer.File;
    dto: UploadImagePipelineDto;
  }): Promise<CreatePipelineResult> {
    let imageOptions: ImageOptions | undefined;
    if (input.dto.options) {
      try {
        imageOptions = JSON.parse(input.dto.options) as ImageOptions;
      } catch {
        throw new BadRequestException(
          'Invalid options field: must be a JSON-encoded object',
        );
      }
    }

    return this.enqueueDirect({
      type: 'IMAGE',
      destinationPrefixRoot: 'images',
      file: input.file,
      originalFilename: input.dto.originalFilename ?? input.file.originalname,
      tenantId: input.dto.tenantId,
      callbackUrl: input.dto.callbackUrl,
      imageOptions,
    });
  }

  private async enqueueRemote(input: {
    type: MediaType;
    destinationPrefixRoot: PrefixRoot;
    presignedUrl: string;
    originalFilename?: string;
    tenantId?: string;
    callbackUrl?: string;
    imageOptions?: ImageOptions;
  }): Promise<CreatePipelineResult> {
    const task = await this.createTask(input);
    const destinationPrefix = `${input.destinationPrefixRoot}/${task.id}/`;
    await this.attachDestination(task.id, destinationPrefix);

    const jobData: IngestJobData = {
      taskId: task.id,
      presignedUrl: input.presignedUrl,
      destination: { bucket: this.s3.bucket, prefix: destinationPrefix },
    };
    await this.enqueueIngest(jobData);

    return { taskId: task.id, status: task.status };
  }

  private async enqueueDirect(input: {
    type: MediaType;
    destinationPrefixRoot: PrefixRoot;
    file: Express.Multer.File;
    originalFilename?: string;
    tenantId?: string;
    callbackUrl?: string;
    imageOptions?: ImageOptions;
  }): Promise<CreatePipelineResult> {
    const task = await this.createTask({
      ...input,
      presignedUrl: undefined,
    });
    const destinationPrefix = `${input.destinationPrefixRoot}/${task.id}/`;
    const sourceKey = `${destinationPrefix}_source/source.bin`;

    await this.s3.uploadFile({
      key: sourceKey,
      filePath: input.file.path,
      contentType: input.file.mimetype,
    });
    await unlink(input.file.path).catch(() => undefined);

    await this.prisma.mediaTask.update({
      where: { id: task.id },
      data: { destinationKey: destinationPrefix, sourceKey },
    });

    const directUpload: DirectUploadDescriptor = {
      byteSize: input.file.size,
      contentType: input.file.mimetype,
    };
    const jobData: IngestJobData = {
      taskId: task.id,
      destination: { bucket: this.s3.bucket, prefix: destinationPrefix },
      directUpload,
    };
    await this.enqueueIngest(jobData);

    return { taskId: task.id, status: task.status };
  }

  private async createTask(input: {
    type: MediaType;
    presignedUrl?: string;
    originalFilename?: string;
    tenantId?: string;
    callbackUrl?: string;
    imageOptions?: ImageOptions;
  }): Promise<{ id: string; status: MediaTaskStatus }> {
    return this.prisma.mediaTask.create({
      data: {
        type: input.type,
        status: 'PENDING',
        progress: 0,
        tenantId: input.tenantId,
        callbackUrl: input.callbackUrl,
        sourceBucket: this.s3.bucket,
        sourceKey: 'pending',
        destinationBucket: this.s3.bucket,
        destinationKey: '',
        metadata: {
          presignedUrl: input.presignedUrl ?? null,
          originalFilename: input.originalFilename ?? null,
          ...(input.imageOptions ? { imageOptions: input.imageOptions } : {}),
        } as unknown as Prisma.InputJsonValue,
      },
    });
  }

  private async attachDestination(
    taskId: string,
    destinationPrefix: string,
  ): Promise<void> {
    await this.prisma.mediaTask.update({
      where: { id: taskId },
      data: {
        destinationKey: destinationPrefix,
        sourceKey: `${destinationPrefix}_source/source.bin`,
      },
    });
  }

  private async enqueueIngest(jobData: IngestJobData): Promise<void> {
    await this.ingestQueue.add('ingest', jobData, {
      removeOnComplete: 100,
      removeOnFail: 500,
      attempts: 3,
      backoff: { type: 'exponential', delay: 5_000 },
    });
  }
}
