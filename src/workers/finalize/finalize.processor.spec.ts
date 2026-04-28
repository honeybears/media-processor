import { getQueueToken } from '@nestjs/bullmq';
import { Test } from '@nestjs/testing';
import { Job } from 'bullmq';
import { PrismaService } from '../../prisma/prisma.service';
import { FinalizeJobData } from '../../pipeline/types';
import { WEBHOOK_QUEUE } from '../../queue/queue.constants';
import { S3Service } from '../../s3/s3.service';
import { FinalizeProcessor } from './finalize.processor';

function makeJob(
  children: Record<string, unknown>,
  prefix = 'videos/t-1/',
): Job<FinalizeJobData> {
  return {
    data: {
      taskId: 't-1',
      destination: { bucket: 'b', prefix },
      source: { bucket: 'b', key: `${prefix}_source/source.bin` },
    },
    getChildrenValues: jest.fn().mockResolvedValue(children),
  } as unknown as Job<FinalizeJobData>;
}

describe('FinalizeProcessor', () => {
  let processor: FinalizeProcessor;
  let putObject: jest.Mock;
  let deletePrefix: jest.Mock;
  let mediaUpdate: jest.Mock;
  let webhookAdd: jest.Mock;

  beforeEach(async () => {
    putObject = jest.fn().mockResolvedValue(undefined);
    deletePrefix = jest.fn().mockResolvedValue(undefined);
    mediaUpdate = jest
      .fn()
      .mockResolvedValue({ tenantId: 'default', callbackUrl: null });
    webhookAdd = jest.fn().mockResolvedValue({});

    const moduleRef = await Test.createTestingModule({
      providers: [
        FinalizeProcessor,
        {
          provide: PrismaService,
          useValue: { mediaTask: { update: mediaUpdate } },
        },
        { provide: S3Service, useValue: { putObject, deletePrefix } },
        {
          provide: getQueueToken(WEBHOOK_QUEUE),
          useValue: { add: webhookAdd },
        },
      ],
    }).compile();

    processor = moduleRef.get(FinalizeProcessor);
  });

  describe('video flow', () => {
    it('writes master.m3u8 with renditions sorted from highest resolution, deletes source staging, marks COMPLETED', async () => {
      const job = makeJob({
        a: {
          rendition: '720p',
          playlistKey: 'videos/t-1/720p/playlist.m3u8',
          segmentKeys: [],
          width: 1280,
          height: 720,
          bandwidth: 2_900_000,
          codecs: 'avc1.4d401f,mp4a.40.2',
        },
        b: {
          rendition: '1080p',
          playlistKey: 'videos/t-1/1080p/playlist.m3u8',
          segmentKeys: [],
          width: 1920,
          height: 1080,
          bandwidth: 5_200_000,
          codecs: 'avc1.640028,mp4a.40.2',
        },
        c: {
          type: 'thumbnail',
          lowResKey: 'videos/t-1/thumbnails/low_res.jpg',
          highResKey: 'videos/t-1/thumbnails/high_res.jpg',
        },
      });

      const result = await processor.process(job);

      expect(putObject).toHaveBeenCalledTimes(1);
      const call = putObject.mock.calls[0][0] as {
        key: string;
        body: string;
        contentType: string;
      };
      expect(call.key).toBe('videos/t-1/master.m3u8');
      expect(call.contentType).toBe('application/vnd.apple.mpegurl');

      const lines = call.body.trim().split('\n');
      expect(lines[0]).toBe('#EXTM3U');
      expect(lines[1]).toBe('#EXT-X-VERSION:3');
      expect(lines[2]).toContain('RESOLUTION=1920x1080');
      expect(lines[2]).toContain('BANDWIDTH=5200000');
      expect(lines[3]).toBe('1080p/playlist.m3u8');
      expect(lines[4]).toContain('RESOLUTION=1280x720');
      expect(lines[5]).toBe('720p/playlist.m3u8');

      expect(deletePrefix).toHaveBeenCalledWith('videos/t-1/_source/');

      expect(mediaUpdate).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 't-1' },
          data: expect.objectContaining({
            status: 'COMPLETED',
            progress: 100,
            finishedAt: expect.any(Date),
          }),
        }),
      );

      expect(result).toEqual({ masterKey: 'videos/t-1/master.m3u8' });
    });

    it('throws when no rendition results are present (video flow)', async () => {
      const job = makeJob({
        c: {
          type: 'thumbnail',
          lowResKey: 'videos/t-1/thumbnails/low_res.jpg',
          highResKey: 'videos/t-1/thumbnails/high_res.jpg',
        },
      });

      await expect(processor.process(job)).rejects.toThrow(
        /No rendition results/,
      );
      expect(putObject).not.toHaveBeenCalled();
      expect(deletePrefix).not.toHaveBeenCalled();
    });
  });

  describe('image flow', () => {
    it('skips master.m3u8, deletes staging, persists image metadata, returns outputKey', async () => {
      const job = makeJob(
        {
          a: {
            type: 'image',
            outputKey: 'images/t-1/output/photo.webp',
            format: 'webp',
            width: 800,
            height: 600,
            byteSize: 12345,
          },
        },
        'images/t-1/',
      );

      const result = await processor.process(job);

      expect(putObject).not.toHaveBeenCalled();
      expect(deletePrefix).toHaveBeenCalledWith('images/t-1/_source/');

      expect(mediaUpdate).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: 't-1' },
          data: expect.objectContaining({
            status: 'COMPLETED',
            progress: 100,
            metadata: expect.objectContaining({
              outputKey: 'images/t-1/output/photo.webp',
              format: 'webp',
              width: 800,
              height: 600,
              byteSize: 12345,
            }),
          }),
        }),
      );

      expect(result).toEqual({ outputKey: 'images/t-1/output/photo.webp' });
    });

    it('enqueues webhook with image payload when callbackUrl is set', async () => {
      mediaUpdate.mockResolvedValueOnce({
        tenantId: 'tenant-x',
        callbackUrl: 'https://hook.example/callback',
      });

      const job = makeJob(
        {
          a: {
            type: 'image',
            outputKey: 'images/t-1/output/photo.webp',
            format: 'webp',
            width: 100,
            height: 50,
            byteSize: 2222,
          },
        },
        'images/t-1/',
      );

      await processor.process(job);

      expect(webhookAdd).toHaveBeenCalledTimes(1);
      const [, payloadArg] = webhookAdd.mock.calls[0] as [
        string,
        { payload: unknown },
        unknown,
      ];
      expect(payloadArg.payload).toEqual(
        expect.objectContaining({
          taskId: 't-1',
          status: 'COMPLETED',
          tenantId: 'tenant-x',
          image: {
            outputKey: 'images/t-1/output/photo.webp',
            format: 'webp',
            width: 100,
            height: 50,
            byteSize: 2222,
          },
        }),
      );
    });
  });
});
