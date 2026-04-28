import { PutObjectCommand } from '@aws-sdk/client-s3';
import { getQueueToken } from '@nestjs/bullmq';
import { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Queue, QueueEvents } from 'bullmq';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/prisma/prisma.service';
import { TRANSCODE_1080P_QUEUE } from '../src/queue/queue.constants';
import { S3Service } from '../src/s3/s3.service';
import { HlsService } from '../src/transcoder/hls.service';
import {
  getRedisConnection,
  makeS3Client,
  s3GetText,
  s3HeadSize,
} from './setup/test-utils';

describe('Transcode 1080p worker (e2e)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let s3Service: S3Service;
  let queue: Queue;
  let queueEvents: QueueEvents;

  const fakeHls = {
    async probe() {
      return { width: 1920, height: 1080, durationSec: 30 };
    },
    async transcodeHls(params: {
      outputDir: string;
      preset: {
        height: number;
        videoBitrateKbps: number;
        audioBitrateKbps: number;
      };
    }) {
      await mkdir(params.outputDir, { recursive: true });
      const playlistFile = join(params.outputDir, 'playlist.m3u8');
      const seg1 = join(params.outputDir, 'segment_001.ts');
      const seg2 = join(params.outputDir, 'segment_002.ts');
      await writeFile(playlistFile, '#EXTM3U\n#EXT-X-VERSION:3\n');
      await writeFile(seg1, Buffer.from([1, 2, 3]));
      await writeFile(seg2, Buffer.from([4, 5, 6]));
      return {
        playlistFile,
        segmentFiles: [seg1, seg2],
        width: 1920,
        height: params.preset.height,
        totalBitrateKbps:
          params.preset.videoBitrateKbps + params.preset.audioBitrateKbps,
      };
    },
    async extractThumbnail() {},
  };

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule.forRole('transcode-1080p')],
    })
      .overrideProvider(HlsService)
      .useValue(fakeHls)
      .compile();
    app = moduleRef.createNestApplication();
    await app.init();
    prisma = app.get(PrismaService);
    s3Service = app.get(S3Service);
    queue = app.get<Queue>(getQueueToken(TRANSCODE_1080P_QUEUE));
    queueEvents = new QueueEvents(TRANSCODE_1080P_QUEUE, {
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

  it('downloads the staged source, runs HLS, uploads playlist + segments to S3', async () => {
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
      const job = await queue.add('transcode-1080p', {
        taskId: task.id,
        source: { bucket: s3Service.bucket, key: sourceKey },
        destination: { bucket: s3Service.bucket, prefix },
      });

      const result = await job.waitUntilFinished(queueEvents);
      expect(result.rendition).toBe('1080p');
      expect(result.height).toBe(1080);
      expect(result.playlistKey).toBe(`${prefix}1080p/playlist.m3u8`);
      expect(result.segmentKeys).toEqual([
        `${prefix}1080p/segment_001.ts`,
        `${prefix}1080p/segment_002.ts`,
      ]);

      const playlistText = await s3GetText(
        s3Client,
        `${prefix}1080p/playlist.m3u8`,
      );
      expect(playlistText).toContain('#EXTM3U');
      expect(await s3HeadSize(s3Client, `${prefix}1080p/segment_001.ts`)).toBe(
        3,
      );
      expect(await s3HeadSize(s3Client, `${prefix}1080p/segment_002.ts`)).toBe(
        3,
      );

      const updated = await prisma.mediaTask.findUniqueOrThrow({
        where: { id: task.id },
      });
      expect(updated.progress).toBe(40);
    } finally {
      s3Client.destroy();
    }
  });
});
