import { getQueueToken } from '@nestjs/bullmq';
import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Queue, QueueEvents } from 'bullmq';
import { createServer, Server } from 'node:http';
import { AddressInfo } from 'node:net';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/prisma/prisma.service';
import {
  FINALIZE_QUEUE,
  INGEST_QUEUE,
  THUMBNAIL_QUEUE,
  TRANSCODE_1080P_QUEUE,
  TRANSCODE_720P_QUEUE,
} from '../src/queue/queue.constants';
import { S3Service } from '../src/s3/s3.service';
import {
  getRedisConnection,
  makeS3Client,
  s3HeadSize,
} from './setup/test-utils';

describe('Ingest worker (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let s3Service: S3Service;
  let ingestQueue: Queue;
  let queueEvents: QueueEvents;
  let httpServer: Server;
  let baseUrl: string;
  let auxQueues: Record<string, Queue>;

  const sourceBytes = Buffer.from('FAKE_SOURCE_BYTES_FOR_INGEST_TEST');

  beforeAll(async () => {
    httpServer = createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'video/mp4' });
      res.end(sourceBytes);
    });
    await new Promise<void>((r) => httpServer.listen(0, '127.0.0.1', r));
    const port = (httpServer.address() as AddressInfo).port;
    baseUrl = `http://127.0.0.1:${port}`;

    const moduleRef = await Test.createTestingModule({
      imports: [AppModule.forRole('ingest')],
    }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);
    s3Service = app.get(S3Service);
    ingestQueue = app.get<Queue>(getQueueToken(INGEST_QUEUE));

    queueEvents = new QueueEvents(INGEST_QUEUE, {
      connection: getRedisConnection(),
    });
    await queueEvents.waitUntilReady();

    auxQueues = Object.fromEntries(
      [
        TRANSCODE_1080P_QUEUE,
        TRANSCODE_720P_QUEUE,
        THUMBNAIL_QUEUE,
        FINALIZE_QUEUE,
      ].map((name) => [
        name,
        new Queue(name, { connection: getRedisConnection() }),
      ]),
    );
  });

  afterAll(async () => {
    await Promise.allSettled([
      ingestQueue.obliterate({ force: true }),
      ...Object.values(auxQueues).map((q) =>
        q.obliterate({ force: true }).then(() => q.close()),
      ),
      queueEvents.close(),
    ]);
    await app.close();
    await new Promise<void>((r) => httpServer.close(() => r()));
  });

  beforeEach(async () => {
    await prisma.mediaTask.deleteMany({});
    await Promise.all([
      ingestQueue.obliterate({ force: true }),
      ...Object.values(auxQueues).map((q) => q.obliterate({ force: true })),
    ]);
  });

  it('downloads the source, uploads it to S3 staging, and fans out a flow tree', async () => {
    const task = await prisma.mediaTask.create({
      data: {
        type: 'VIDEO',
        status: 'PENDING',
        progress: 0,
        sourceBucket: s3Service.bucket,
        sourceKey: 'pending',
        destinationBucket: s3Service.bucket,
        destinationKey: '',
        metadata: {},
      },
    });
    const destinationPrefix = `videos/${task.id}/`;
    await prisma.mediaTask.update({
      where: { id: task.id },
      data: {
        destinationKey: destinationPrefix,
        sourceKey: `${destinationPrefix}_source/source.bin`,
      },
    });

    const job = await ingestQueue.add('ingest', {
      taskId: task.id,
      presignedUrl: `${baseUrl}/file.mp4`,
      destination: { bucket: s3Service.bucket, prefix: destinationPrefix },
    });

    await job.waitUntilFinished(queueEvents);

    const s3Client = makeS3Client();
    try {
      const size = await s3HeadSize(
        s3Client,
        `${destinationPrefix}_source/source.bin`,
      );
      expect(size).toBe(sourceBytes.length);
    } finally {
      s3Client.destroy();
    }

    const updated = await prisma.mediaTask.findUniqueOrThrow({
      where: { id: task.id },
    });
    expect(updated.status).toBe('PROCESSING');

    const finalizeJobs = await auxQueues[FINALIZE_QUEUE].getJobs([
      'waiting',
      'waiting-children',
      'delayed',
    ]);
    const t1080 = await auxQueues[TRANSCODE_1080P_QUEUE].getJobs([
      'waiting',
      'delayed',
    ]);
    const t720 = await auxQueues[TRANSCODE_720P_QUEUE].getJobs([
      'waiting',
      'delayed',
    ]);
    const thumbs = await auxQueues[THUMBNAIL_QUEUE].getJobs([
      'waiting',
      'delayed',
    ]);

    expect(finalizeJobs).toHaveLength(1);
    expect(t1080).toHaveLength(1);
    expect(t720).toHaveLength(1);
    expect(thumbs).toHaveLength(1);

    const childTaskIds = [t1080[0], t720[0], thumbs[0]].map(
      (j) => j.data.taskId,
    );
    expect(childTaskIds.every((id) => id === task.id)).toBe(true);
  });
});
