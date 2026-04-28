import { GetObjectCommand } from '@aws-sdk/client-s3';
import { getQueueToken } from '@nestjs/bullmq';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Queue } from 'bullmq';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/prisma/prisma.service';
import { INGEST_QUEUE } from '../src/queue/queue.constants';
import { makeS3Client } from './setup/test-utils';

describe('Pipeline API (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let ingestQueue: Queue;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule.forRole('api')],
    }).compile();

    app = moduleRef.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({ whitelist: true, transform: true }),
    );
    await app.init();
    prisma = app.get(PrismaService);
    ingestQueue = app.get<Queue>(getQueueToken(INGEST_QUEUE));
  });

  afterAll(async () => {
    await ingestQueue.obliterate({ force: true });
    await app.close();
  });

  beforeEach(async () => {
    await prisma.mediaTask.deleteMany({});
    await ingestQueue.obliterate({ force: true });
  });

  it('persists a MediaTask and enqueues a real ingest job', async () => {
    const res = await request(app.getHttpServer())
      .post('/videos')
      .send({
        presignedUrl: 'https://example.com/abc.mp4',
        originalFilename: 'abc.mp4',
      })
      .expect(201);

    expect(res.body.taskId).toEqual(expect.any(String));
    expect(res.body.status).toBe('PENDING');

    const task = await prisma.mediaTask.findUniqueOrThrow({
      where: { id: res.body.taskId },
    });
    expect(task.destinationKey).toBe(`videos/${task.id}/`);
    expect(task.sourceKey).toBe(`videos/${task.id}/_source/source.bin`);
    expect((task.metadata as Record<string, unknown>).presignedUrl).toBe(
      'https://example.com/abc.mp4',
    );

    const jobs = await ingestQueue.getJobs(['waiting', 'delayed', 'active']);
    expect(jobs).toHaveLength(1);
    expect(jobs[0].data.taskId).toBe(task.id);
    expect(jobs[0].data.presignedUrl).toBe('https://example.com/abc.mp4');
  });

  it('rejects malformed presignedUrl with 400 and creates nothing', async () => {
    await request(app.getHttpServer())
      .post('/videos')
      .send({ presignedUrl: 'not-a-url' })
      .expect(400);

    expect(await prisma.mediaTask.count()).toBe(0);
    const counts = await ingestQueue.getJobCounts();
    expect(counts.waiting).toBe(0);
  });

  it('rejects empty body with 400', async () => {
    await request(app.getHttpServer()).post('/videos').send({}).expect(400);
    expect(await prisma.mediaTask.count()).toBe(0);
  });

  it('strips unknown fields via whitelist', async () => {
    await request(app.getHttpServer())
      .post('/videos')
      .send({
        presignedUrl: 'https://example.com/abc.mp4',
        evil: 'should-not-leak',
      })
      .expect(201);

    const jobs = await ingestQueue.getJobs(['waiting']);
    expect(jobs[0].data).not.toHaveProperty('evil');
  });

  describe('POST /images', () => {
    it('persists an IMAGE MediaTask under images/ prefix and stores imageOptions', async () => {
      const res = await request(app.getHttpServer())
        .post('/images')
        .send({
          presignedUrl: 'https://example.com/photo.heic',
          originalFilename: 'photo.heic',
          options: {
            crop: { left: 5, top: 10, width: 100, height: 50 },
            resize: { width: 800, fit: 'inside' },
            format: 'webp',
            quality: 80,
          },
        })
        .expect(201);

      const task = await prisma.mediaTask.findUniqueOrThrow({
        where: { id: res.body.taskId },
      });
      expect(task.type).toBe('IMAGE');
      expect(task.destinationKey).toBe(`images/${task.id}/`);
      expect(task.sourceKey).toBe(`images/${task.id}/_source/source.bin`);

      const metadata = task.metadata as Record<string, unknown>;
      expect(metadata.presignedUrl).toBe('https://example.com/photo.heic');
      expect(metadata.imageOptions).toEqual({
        crop: { left: 5, top: 10, width: 100, height: 50 },
        resize: { width: 800, fit: 'inside' },
        format: 'webp',
        quality: 80,
      });

      const jobs = await ingestQueue.getJobs(['waiting', 'delayed', 'active']);
      expect(jobs).toHaveLength(1);
      expect(jobs[0].data.taskId).toBe(task.id);
      expect(jobs[0].data.destination.prefix).toBe(`images/${task.id}/`);
    });

    it('accepts a request without options and stores no imageOptions', async () => {
      const res = await request(app.getHttpServer())
        .post('/images')
        .send({ presignedUrl: 'https://example.com/photo.png' })
        .expect(201);

      const task = await prisma.mediaTask.findUniqueOrThrow({
        where: { id: res.body.taskId },
      });
      expect(task.type).toBe('IMAGE');
      const metadata = task.metadata as Record<string, unknown>;
      expect(metadata).not.toHaveProperty('imageOptions');
    });

    it('rejects invalid options (negative crop) with 400 and creates nothing', async () => {
      await request(app.getHttpServer())
        .post('/images')
        .send({
          presignedUrl: 'https://example.com/photo.png',
          options: {
            crop: { left: -1, top: 0, width: 100, height: 100 },
          },
        })
        .expect(400);

      expect(await prisma.mediaTask.count()).toBe(0);
    });

    it('rejects unsupported format with 400', async () => {
      await request(app.getHttpServer())
        .post('/images')
        .send({
          presignedUrl: 'https://example.com/photo.png',
          options: { format: 'gif' },
        })
        .expect(400);
    });

    it('rejects malformed presignedUrl with 400', async () => {
      await request(app.getHttpServer())
        .post('/images')
        .send({ presignedUrl: 'not-a-url' })
        .expect(400);
    });

    it('strips unknown fields via whitelist (image)', async () => {
      await request(app.getHttpServer())
        .post('/images')
        .send({
          presignedUrl: 'https://example.com/photo.png',
          evil: 'should-not-leak',
        })
        .expect(201);

      const jobs = await ingestQueue.getJobs(['waiting']);
      expect(jobs[0].data).not.toHaveProperty('evil');
    });
  });

  describe('POST /videos/upload', () => {
    it('uploads file directly, stages it in S3, and enqueues ingest with directUpload', async () => {
      const fileBytes = Buffer.from('fake-mp4-bytes');

      const res = await request(app.getHttpServer())
        .post('/videos/upload')
        .field('tenantId', 'tenant-x')
        .attach('file', fileBytes, {
          filename: 'movie.mp4',
          contentType: 'video/mp4',
        })
        .expect(201);

      const task = await prisma.mediaTask.findUniqueOrThrow({
        where: { id: res.body.taskId },
      });
      expect(task.type).toBe('VIDEO');
      expect(task.tenantId).toBe('tenant-x');
      expect(task.sourceKey).toBe(`videos/${task.id}/_source/source.bin`);

      const s3 = makeS3Client();
      try {
        const obj = await s3.send(
          new GetObjectCommand({
            Bucket: process.env.S3_BUCKET,
            Key: task.sourceKey,
          }),
        );
        const body = await obj.Body!.transformToByteArray();
        expect(Buffer.from(body).equals(fileBytes)).toBe(true);
      } finally {
        s3.destroy();
      }

      const jobs = await ingestQueue.getJobs(['waiting', 'delayed', 'active']);
      expect(jobs).toHaveLength(1);
      const data = jobs[0].data;
      expect(data.taskId).toBe(task.id);
      expect(data.directUpload).toEqual({
        byteSize: fileBytes.length,
        contentType: 'video/mp4',
      });
      expect(data).not.toHaveProperty('presignedUrl');
    });

    it('rejects request without file with 400', async () => {
      await request(app.getHttpServer())
        .post('/videos/upload')
        .field('tenantId', 'tenant-x')
        .expect(400);

      expect(await prisma.mediaTask.count()).toBe(0);
    });
  });

  describe('POST /images/upload', () => {
    it('uploads file directly with options JSON and stores them in metadata', async () => {
      const fileBytes = Buffer.from('fake-png-bytes');

      const res = await request(app.getHttpServer())
        .post('/images/upload')
        .field('options', JSON.stringify({ format: 'webp', quality: 75 }))
        .attach('file', fileBytes, {
          filename: 'photo.png',
          contentType: 'image/png',
        })
        .expect(201);

      const task = await prisma.mediaTask.findUniqueOrThrow({
        where: { id: res.body.taskId },
      });
      expect(task.type).toBe('IMAGE');
      expect(task.sourceKey).toBe(`images/${task.id}/_source/source.bin`);

      const metadata = task.metadata as Record<string, unknown>;
      expect(metadata.imageOptions).toEqual({ format: 'webp', quality: 75 });
      expect(metadata.originalFilename).toBe('photo.png');

      const jobs = await ingestQueue.getJobs(['waiting', 'delayed', 'active']);
      const data = jobs[0].data;
      expect(data.directUpload).toEqual({
        byteSize: fileBytes.length,
        contentType: 'image/png',
      });
    });

    it('rejects malformed options JSON with 400 and creates nothing', async () => {
      await request(app.getHttpServer())
        .post('/images/upload')
        .field('options', 'not-json')
        .attach('file', Buffer.from('x'), {
          filename: 'photo.png',
          contentType: 'image/png',
        })
        .expect(400);

      expect(await prisma.mediaTask.count()).toBe(0);
    });

    it('accepts upload without options', async () => {
      const fileBytes = Buffer.from('fake-png-bytes');
      const res = await request(app.getHttpServer())
        .post('/images/upload')
        .attach('file', fileBytes, {
          filename: 'photo.png',
          contentType: 'image/png',
        })
        .expect(201);

      const task = await prisma.mediaTask.findUniqueOrThrow({
        where: { id: res.body.taskId },
      });
      const metadata = task.metadata as Record<string, unknown>;
      expect(metadata).not.toHaveProperty('imageOptions');
    });
  });
});
