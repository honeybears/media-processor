import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { mkdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, join } from 'node:path';
import { buffer as readBuffer } from 'node:stream/consumers';
import sharp from 'sharp';
import { PrismaService } from '../../prisma/prisma.service';
import {
  ImageJobData,
  ImageOptions,
  ImageOutputFormat,
  ImageResult,
} from '../../pipeline/types';
import { IMAGE_QUEUE } from '../../queue/queue.constants';
import { S3Service } from '../../s3/s3.service';

const FORMAT_CONTENT_TYPE: Record<ImageOutputFormat, string> = {
  jpeg: 'image/jpeg',
  png: 'image/png',
  webp: 'image/webp',
  avif: 'image/avif',
};

@Processor(IMAGE_QUEUE)
export class ImageProcessor extends WorkerHost {
  private readonly logger = new Logger(ImageProcessor.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly s3: S3Service,
  ) {
    super();
  }

  async process(job: Job<ImageJobData>): Promise<ImageResult> {
    const { taskId, source, destination, options } = job.data;
    const workDir = join(tmpdir(), `media-${taskId}-image`);
    await mkdir(workDir, { recursive: true });

    this.logger.log(`Image start task=${taskId}`);

    try {
      const stream = await this.s3.getObjectStream(source.key);
      const inputBuffer = await readBuffer(stream);

      const { buffer, format, info } = await this.transform(
        inputBuffer,
        options,
      );

      const ext = format;
      const baseName = stripExt(source.originalFilename ?? 'output');
      const outputKey = `${destination.prefix}output/${baseName}.${ext}`;
      const localOutputPath = join(workDir, `output.${ext}`);
      await writeFile(localOutputPath, buffer);
      const { size } = await stat(localOutputPath);

      await this.s3.uploadFile({
        key: outputKey,
        filePath: localOutputPath,
        contentType: FORMAT_CONTENT_TYPE[format],
      });

      await this.prisma.mediaTask.update({
        where: { id: taskId },
        data: { progress: { increment: 80 } },
      });

      this.logger.log(
        `Image done task=${taskId} ${info.width}x${info.height} format=${format} bytes=${size}`,
      );

      return {
        type: 'image',
        outputKey,
        format,
        width: info.width,
        height: info.height,
        byteSize: size,
      };
    } finally {
      await rm(workDir, { recursive: true, force: true });
    }
  }

  private async transform(
    input: Buffer,
    options: ImageOptions,
  ): Promise<{
    buffer: Buffer;
    format: ImageOutputFormat;
    info: sharp.OutputInfo;
  }> {
    let pipeline = sharp(input, { failOn: 'error' }).rotate();

    if (options.crop) {
      const { left, top, width, height } = options.crop;
      pipeline = pipeline.extract({ left, top, width, height });
    }

    if (options.resize) {
      pipeline = pipeline.resize({
        width: options.resize.width,
        height: options.resize.height,
        fit: options.resize.fit,
        withoutEnlargement: true,
      });
    }

    const format = options.format ?? 'jpeg';
    const quality = options.quality;

    switch (format) {
      case 'jpeg':
        pipeline = pipeline.jpeg(quality ? { quality } : undefined);
        break;
      case 'png':
        pipeline = pipeline.png(
          quality ? { compressionLevel: clampPng(quality) } : undefined,
        );
        break;
      case 'webp':
        pipeline = pipeline.webp(quality ? { quality } : undefined);
        break;
      case 'avif':
        pipeline = pipeline.avif(quality ? { quality } : undefined);
        break;
    }

    const { data, info } = await pipeline.toBuffer({ resolveWithObject: true });
    return { buffer: data, format, info };
  }
}

function stripExt(name: string): string {
  const ext = extname(name);
  return ext ? name.slice(0, -ext.length) : name;
}

function clampPng(quality: number): number {
  const level = Math.round(9 - (quality / 100) * 9);
  return Math.max(0, Math.min(9, level));
}
