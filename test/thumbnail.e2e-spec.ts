import { PutObjectCommand } from '@aws-sdk/client-s3';
import { getQueueToken } from '@nestjs/bullmq';
import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Queue, QueueEvents } from 'bullmq';
import { writeFile } from 'node:fs/promises';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/prisma/prisma.service';
import { THUMBNAIL_QUEUE } from '../src/queue/queue.constants';
import { S3Service } from '../src/s3/s3.service';
import { HlsService } from '../src/transcoder/hls.service';
import {
  getRedisConnection,
  makeS3Client,
  s3HeadSize,
} from './setup/test-utils';

describe('Thumbnail worker (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let s3Service: S3Service;
  let queue: Queue;
  let queueEvents: QueueEvents;

  const fakeHls = {
    async probe() {
      return { width: 1920, height: 1080, durationSec: 60 };
    },
    async transcodeHls() {
      throw new Error('not used in thumbnail test');
    },
    async extractThumbnail(params: { outputPath: string }) {
      await writeFile(params.outputPath, Buffer.from('fake-jpeg-bytes'));
    },
  };

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule.forRole('thumbnail')],
    })
      .overrideProvider(HlsService)
      .useValue(fakeHls)
      .compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);
    s3Service = app.get(S3Service);
    queue = app.get<Queue>(getQueueToken(THUMBNAIL_QUEUE));
    queueEvents = new QueueEvents(THUMBNAIL_QUEUE, {
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

  it('extracts low/high resolution thumbnails and uploads them to S3', async () => {
    const task = await prisma.mediaTask.create({
      data: {
        type: 'VIDEO',
        status: 'PROCESSING',
        progress: 10,
        sourceBucket: s3Service.bucket,
        sourceKey: '',
        destinationBucket: s3Service.bucket,
        destinationKey: '',
        metadata: {},
      },
    });
    const prefix = `videos/${task.id}/`;
    const sourceKey = `${prefix}_source/source.bin`;
    await prisma.mediaTask.update({
      where: { id: task.id },
      data: { sourceKey, destinationKey: prefix },
    });

    const s3Client = makeS3Client();
    await s3Client.send(
      new PutObjectCommand({
        Bucket: process.env.S3_BUCKET,
        Key: sourceKey,
        Body: Buffer.from('fake source bytes'),
      }),
    );

    try {
      const job = await queue.add('thumbnail', {
        taskId: task.id,
        source: { bucket: s3Service.bucket, key: sourceKey },
        destination: { bucket: s3Service.bucket, prefix },
      });

      const result = await job.waitUntilFinished(queueEvents);
      expect(result).toEqual({
        type: 'thumbnail',
        lowResKey: `${prefix}thumbnails/low_res.jpg`,
        highResKey: `${prefix}thumbnails/high_res.jpg`,
      });

      expect(
        await s3HeadSize(s3Client, `${prefix}thumbnails/low_res.jpg`),
      ).toBeGreaterThan(0);
      expect(
        await s3HeadSize(s3Client, `${prefix}thumbnails/high_res.jpg`),
      ).toBeGreaterThan(0);

      const updated = await prisma.mediaTask.findUniqueOrThrow({
        where: { id: task.id },
      });
      expect(updated.progress).toBe(20);
    } finally {
      s3Client.destroy();
    }
  });
});
