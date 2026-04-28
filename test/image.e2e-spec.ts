import { PutObjectCommand } from '@aws-sdk/client-s3';
import { getQueueToken } from '@nestjs/bullmq';
import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Queue, QueueEvents } from 'bullmq';
import sharp from 'sharp';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/prisma/prisma.service';
import { IMAGE_QUEUE } from '../src/queue/queue.constants';
import { S3Service } from '../src/s3/s3.service';
import {
  getRedisConnection,
  makeS3Client,
  s3HeadSize,
} from './setup/test-utils';

async function makePng(
  width: number,
  height: number,
  rgb: { r: number; g: number; b: number } = { r: 200, g: 50, b: 50 },
): Promise<Buffer> {
  return sharp({
    create: { width, height, channels: 3, background: rgb },
  })
    .png()
    .toBuffer();
}

describe('Image worker (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let s3Service: S3Service;
  let queue: Queue;
  let queueEvents: QueueEvents;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule.forRole('image')],
    }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);
    s3Service = app.get(S3Service);
    queue = app.get<Queue>(getQueueToken(IMAGE_QUEUE));
    queueEvents = new QueueEvents(IMAGE_QUEUE, {
      connection: getRedisConnection(),
    });
    await queueEvents.waitUntilReady();
  });

  afterAll(async () => {
    await queue.obliterate({ force: true });
    await queueEvents.close();
    await app.close();
  });

  beforeEach(async () => {
    await prisma.mediaTask.deleteMany({});
    await queue.obliterate({ force: true });
  });

  async function seedSourceImage(
    prefix: string,
    body: Buffer,
  ): Promise<string> {
    const sourceKey = `${prefix}_source/source.bin`;
    const s3Client = makeS3Client();
    try {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: process.env.S3_BUCKET,
          Key: sourceKey,
          Body: body,
        }),
      );
      return sourceKey;
    } finally {
      s3Client.destroy();
    }
  }

  async function seedTask(
    prefix: string,
    sourceKey: string,
  ): Promise<{ id: string }> {
    const task = await prisma.mediaTask.create({
      data: {
        type: 'IMAGE',
        status: 'PROCESSING',
        progress: 10,
        sourceBucket: s3Service.bucket,
        sourceKey,
        destinationBucket: s3Service.bucket,
        destinationKey: prefix,
        metadata: {},
      },
    });
    return { id: task.id };
  }

  it('crops, resizes and converts to webp; uploads result to S3', async () => {
    const png = await makePng(800, 600);
    const prefix = `images/e2e-1/`;
    const sourceKey = await seedSourceImage(prefix, png);
    const task = await seedTask(prefix, sourceKey);

    const job = await queue.add('image', {
      taskId: task.id,
      source: {
        bucket: s3Service.bucket,
        key: sourceKey,
        originalFilename: 'sample.png',
      },
      destination: { bucket: s3Service.bucket, prefix },
      options: {
        crop: { left: 50, top: 50, width: 400, height: 400 },
        resize: { width: 200, height: 200, fit: 'cover' },
        format: 'webp',
        quality: 80,
      },
    });

    const result = await job.waitUntilFinished(queueEvents);
    expect(result).toEqual(
      expect.objectContaining({
        type: 'image',
        outputKey: `${prefix}output/sample.webp`,
        format: 'webp',
        width: 200,
        height: 200,
      }),
    );

    const s3Client = makeS3Client();
    try {
      const size = await s3HeadSize(s3Client, `${prefix}output/sample.webp`);
      expect(size).toBeGreaterThan(0);
    } finally {
      s3Client.destroy();
    }

    const updated = await prisma.mediaTask.findUniqueOrThrow({
      where: { id: task.id },
    });
    expect(updated.progress).toBe(90);
  });

  it('passes through with default jpeg format when no options provided', async () => {
    const png = await makePng(64, 64);
    const prefix = `images/e2e-2/`;
    const sourceKey = await seedSourceImage(prefix, png);
    const task = await seedTask(prefix, sourceKey);

    const job = await queue.add('image', {
      taskId: task.id,
      source: {
        bucket: s3Service.bucket,
        key: sourceKey,
        originalFilename: 'thumb.png',
      },
      destination: { bucket: s3Service.bucket, prefix },
      options: {},
    });

    const result = await job.waitUntilFinished(queueEvents);
    expect(result.format).toBe('jpeg');
    expect(result.outputKey).toBe(`${prefix}output/thumb.jpeg`);
    expect(result.width).toBe(64);
    expect(result.height).toBe(64);
  });

  it('falls back to "output" basename when originalFilename is missing', async () => {
    const png = await makePng(32, 32);
    const prefix = `images/e2e-3/`;
    const sourceKey = await seedSourceImage(prefix, png);
    const task = await seedTask(prefix, sourceKey);

    const job = await queue.add('image', {
      taskId: task.id,
      source: { bucket: s3Service.bucket, key: sourceKey },
      destination: { bucket: s3Service.bucket, prefix },
      options: { format: 'png' },
    });

    const result = await job.waitUntilFinished(queueEvents);
    expect(result.outputKey).toBe(`${prefix}output/output.png`);
  });
});
