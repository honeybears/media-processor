import { Test } from '@nestjs/testing';
import { Job } from 'bullmq';
import { Readable } from 'node:stream';
import { ThumbnailJobData } from '../../pipeline/types';
import { PrismaService } from '../../prisma/prisma.service';
import { S3Service } from '../../s3/s3.service';
import { HlsService } from '../../transcoder/hls.service';
import { ThumbnailProcessor } from './thumbnail.processor';

jest.mock('node:fs/promises', () => ({
  mkdir: jest.fn().mockResolvedValue(undefined),
  rm: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('node:fs', () => ({
  createWriteStream: jest.fn(() => ({})),
}));

jest.mock('node:stream/promises', () => ({
  pipeline: jest.fn().mockResolvedValue(undefined),
}));

describe('ThumbnailProcessor', () => {
  let processor: ThumbnailProcessor;
  let getObjectStream: jest.Mock;
  let uploadFile: jest.Mock;
  let probe: jest.Mock;
  let extractThumbnail: jest.Mock;
  let mediaUpdate: jest.Mock;

  beforeEach(async () => {
    getObjectStream = jest.fn().mockResolvedValue(Readable.from([]));
    uploadFile = jest.fn().mockResolvedValue(undefined);
    probe = jest.fn().mockResolvedValue({
      width: 1920,
      height: 1080,
      durationSec: 60,
    });
    extractThumbnail = jest.fn().mockResolvedValue(undefined);
    mediaUpdate = jest.fn().mockResolvedValue({});

    const moduleRef = await Test.createTestingModule({
      providers: [
        ThumbnailProcessor,
        {
          provide: PrismaService,
          useValue: { mediaTask: { update: mediaUpdate } },
        },
        { provide: S3Service, useValue: { getObjectStream, uploadFile } },
        { provide: HlsService, useValue: { probe, extractThumbnail } },
      ],
    }).compile();

    processor = moduleRef.get(ThumbnailProcessor);
  });

  it('extracts low/high resolution thumbnails at 10% mark and uploads them', async () => {
    const job = {
      data: {
        taskId: 't-1',
        source: { bucket: 'b', key: 'videos/t-1/_source/source.bin' },
        destination: { bucket: 'b', prefix: 'videos/t-1/' },
      },
    } as Job<ThumbnailJobData>;

    const result = await processor.process(job);

    expect(extractThumbnail).toHaveBeenCalledWith(
      expect.objectContaining({ width: 320, timestampSec: 6 }),
    );
    expect(extractThumbnail).toHaveBeenCalledWith(
      expect.objectContaining({ width: 1280, timestampSec: 6 }),
    );

    expect(uploadFile).toHaveBeenCalledWith(
      expect.objectContaining({
        key: 'videos/t-1/thumbnails/low_res.jpg',
        contentType: 'image/jpeg',
      }),
    );
    expect(uploadFile).toHaveBeenCalledWith(
      expect.objectContaining({
        key: 'videos/t-1/thumbnails/high_res.jpg',
        contentType: 'image/jpeg',
      }),
    );

    expect(result).toEqual({
      type: 'thumbnail',
      lowResKey: 'videos/t-1/thumbnails/low_res.jpg',
      highResKey: 'videos/t-1/thumbnails/high_res.jpg',
    });
  });

  it('caps capture timestamp at 10 seconds for long videos', async () => {
    probe.mockResolvedValueOnce({
      width: 1920,
      height: 1080,
      durationSec: 600,
    });

    const job = {
      data: {
        taskId: 't-1',
        source: { bucket: 'b', key: 'videos/t-1/_source/source.bin' },
        destination: { bucket: 'b', prefix: 'videos/t-1/' },
      },
    } as Job<ThumbnailJobData>;

    await processor.process(job);

    expect(extractThumbnail.mock.calls[0][0].timestampSec).toBe(10);
  });
});
