import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { FlowProducer, QueueEvents, Worker } from 'bullmq';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/prisma/prisma.service';
import {
  FINALIZE_QUEUE,
  THUMBNAIL_QUEUE,
  TRANSCODE_1080P_QUEUE,
  TRANSCODE_720P_QUEUE,
} from '../src/queue/queue.constants';
import { S3Service } from '../src/s3/s3.service';
import { getRedisConnection, s3GetText } from './setup/test-utils';
import { makeS3Client } from './setup/test-utils';

describe('Finalize worker (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let s3Service: S3Service;
  let queueEvents: QueueEvents;
  let auxWorkers: Worker[];
  let flow: FlowProducer;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule.forRole('finalize')],
    }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);
    s3Service = app.get(S3Service);

    const conn = getRedisConnection();
    queueEvents = new QueueEvents(FINALIZE_QUEUE, { connection: conn });
    await queueEvents.waitUntilReady();

    auxWorkers = [
      new Worker(
        TRANSCODE_1080P_QUEUE,
        async () => ({
          rendition: '1080p',
          playlistKey: 'videos/T-1/1080p/playlist.m3u8',
          segmentKeys: ['videos/T-1/1080p/segment_001.ts'],
          width: 1920,
          height: 1080,
          bandwidth: 5_200_000,
          codecs: 'avc1.640028,mp4a.40.2',
        }),
        { connection: conn },
      ),
      new Worker(
        TRANSCODE_720P_QUEUE,
        async () => ({
          rendition: '720p',
          playlistKey: 'videos/T-1/720p/playlist.m3u8',
          segmentKeys: ['videos/T-1/720p/segment_001.ts'],
          width: 1280,
          height: 720,
          bandwidth: 2_900_000,
          codecs: 'avc1.4d401f,mp4a.40.2',
        }),
        { connection: conn },
      ),
      new Worker(
        THUMBNAIL_QUEUE,
        async () => ({
          type: 'thumbnail',
          lowResKey: 'videos/T-1/thumbnails/low_res.jpg',
          highResKey: 'videos/T-1/thumbnails/high_res.jpg',
        }),
        { connection: conn },
      ),
    ];
    await Promise.all(auxWorkers.map((w) => w.waitUntilReady()));

    flow = new FlowProducer({ connection: conn });
  });

  afterAll(async () => {
    await Promise.allSettled(auxWorkers.map((w) => w.close()));
    await flow.close();
    await queueEvents.close();
    await app.close();
  });

  beforeEach(async () => {
    await prisma.mediaTask.deleteMany({});
  });

  it('collects child rendition results, writes master.m3u8, and marks the MediaTask COMPLETED', async () => {
    const task = await prisma.mediaTask.create({
      data: {
        type: 'VIDEO',
        status: 'PROCESSING',
        progress: 70,
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

    const tree = await flow.add({
      name: 'finalize',
      queueName: FINALIZE_QUEUE,
      data: {
        taskId: task.id,
        source: { bucket: s3Service.bucket, key: sourceKey },
        destination: { bucket: s3Service.bucket, prefix },
      },
      children: [
        {
          name: 'transcode-1080p',
          queueName: TRANSCODE_1080P_QUEUE,
          data: { taskId: task.id },
        },
        {
          name: 'transcode-720p',
          queueName: TRANSCODE_720P_QUEUE,
          data: { taskId: task.id },
        },
        {
          name: 'thumbnail',
          queueName: THUMBNAIL_QUEUE,
          data: { taskId: task.id },
        },
      ],
    });

    const result = await tree.job.waitUntilFinished(queueEvents);
    expect(result.masterKey).toBe(`${prefix}master.m3u8`);

    const s3Client = makeS3Client();
    try {
      const master = await s3GetText(s3Client, `${prefix}master.m3u8`);
      const lines = master.trim().split('\n');
      expect(lines[0]).toBe('#EXTM3U');
      expect(lines[2]).toContain('RESOLUTION=1920x1080');
      expect(lines[3]).toBe('1080p/playlist.m3u8');
      expect(lines[4]).toContain('RESOLUTION=1280x720');
      expect(lines[5]).toBe('720p/playlist.m3u8');
    } finally {
      s3Client.destroy();
    }

    const updated = await prisma.mediaTask.findUniqueOrThrow({
      where: { id: task.id },
    });
    expect(updated.status).toBe('COMPLETED');
    expect(updated.progress).toBe(100);
    expect(updated.finishedAt).toBeInstanceOf(Date);
  });
});
