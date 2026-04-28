import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { createWriteStream } from 'node:fs';
import { mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { PrismaService } from '../../prisma/prisma.service';
import { ThumbnailJobData, ThumbnailResult } from '../../pipeline/types';
import { THUMBNAIL_QUEUE } from '../../queue/queue.constants';
import { S3Service } from '../../s3/s3.service';
import { HlsService } from '../../transcoder/hls.service';

@Processor(THUMBNAIL_QUEUE)
export class ThumbnailProcessor extends WorkerHost {
  private readonly logger = new Logger(ThumbnailProcessor.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly s3: S3Service,
    private readonly hls: HlsService,
  ) {
    super();
  }

  async process(job: Job<ThumbnailJobData>): Promise<ThumbnailResult> {
    const { taskId, source, destination } = job.data;
    const workDir = join(tmpdir(), `media-${taskId}-thumb`);
    const inputPath = join(workDir, basename(source.key));
    const lowResPath = join(workDir, 'low_res.jpg');
    const highResPath = join(workDir, 'high_res.jpg');

    await mkdir(workDir, { recursive: true });
    this.logger.log(`Thumbnail start task=${taskId}`);

    try {
      const stream = await this.s3.getObjectStream(source.key);
      await pipeline(stream, createWriteStream(inputPath));

      const probe = await this.hls.probe(inputPath);
      const captureAt = Math.max(0, Math.min(probe.durationSec * 0.1, 10));

      await this.hls.extractThumbnail({
        inputPath,
        outputPath: lowResPath,
        width: 320,
        timestampSec: captureAt,
      });
      await this.hls.extractThumbnail({
        inputPath,
        outputPath: highResPath,
        width: 1280,
        timestampSec: captureAt,
      });

      const lowResKey = `${destination.prefix}thumbnails/low_res.jpg`;
      const highResKey = `${destination.prefix}thumbnails/high_res.jpg`;

      await this.s3.uploadFile({
        key: lowResKey,
        filePath: lowResPath,
        contentType: 'image/jpeg',
      });
      await this.s3.uploadFile({
        key: highResKey,
        filePath: highResPath,
        contentType: 'image/jpeg',
      });

      await this.prisma.mediaTask.update({
        where: { id: taskId },
        data: { progress: { increment: 10 } },
      });

      this.logger.log(`Thumbnail done task=${taskId}`);
      return { type: 'thumbnail', lowResKey, highResKey };
    } finally {
      await rm(workDir, { recursive: true, force: true });
    }
  }
}
