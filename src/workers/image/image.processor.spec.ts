import { Test } from '@nestjs/testing';
import { Job } from 'bullmq';
import { readFile } from 'node:fs/promises';
import { Readable } from 'node:stream';
import sharp from 'sharp';
import { PrismaService } from '../../prisma/prisma.service';
import { ImageJobData } from '../../pipeline/types';
import { S3Service } from '../../s3/s3.service';
import { ImageProcessor } from './image.processor';

async function makeRedPng(width: number, height: number): Promise<Buffer> {
  return sharp({
    create: {
      width,
      height,
      channels: 3,
      background: { r: 255, g: 0, b: 0 },
    },
  })
    .png()
    .toBuffer();
}

interface UploadCall {
  key: string;
  contentType: string;
  buffer: Buffer;
}

describe('ImageProcessor', () => {
  let processor: ImageProcessor;
  let mediaUpdate: jest.Mock;
  let getObjectStream: jest.Mock;
  let uploadCalls: UploadCall[];
  let uploadFile: jest.Mock;

  beforeEach(async () => {
    mediaUpdate = jest.fn().mockResolvedValue({});
    getObjectStream = jest.fn();
    uploadCalls = [];
    uploadFile = jest
      .fn()
      .mockImplementation(
        async (params: {
          key: string;
          filePath: string;
          contentType: string;
        }) => {
          const buffer = await readFile(params.filePath);
          uploadCalls.push({
            key: params.key,
            contentType: params.contentType,
            buffer,
          });
        },
      );

    const moduleRef = await Test.createTestingModule({
      providers: [
        ImageProcessor,
        {
          provide: PrismaService,
          useValue: { mediaTask: { update: mediaUpdate } },
        },
        { provide: S3Service, useValue: { getObjectStream, uploadFile } },
      ],
    }).compile();

    processor = moduleRef.get(ImageProcessor);
  });

  function makeJob(
    input: Buffer,
    options: ImageJobData['options'],
  ): Job<ImageJobData> {
    getObjectStream.mockResolvedValue(Readable.from(input));
    return {
      data: {
        taskId: 't-img',
        source: {
          bucket: 'b',
          key: 'images/t-img/_source/source.bin',
          originalFilename: 'photo.png',
        },
        destination: { bucket: 'b', prefix: 'images/t-img/' },
        options,
      },
    } as Job<ImageJobData>;
  }

  it('crops the image to the requested rectangle', async () => {
    const png = await makeRedPng(400, 300);
    const job = makeJob(png, {
      crop: { left: 10, top: 20, width: 100, height: 50 },
      format: 'png',
    });

    const result = await processor.process(job);

    expect(result.width).toBe(100);
    expect(result.height).toBe(50);
    expect(result.format).toBe('png');

    expect(uploadCalls).toHaveLength(1);
    const meta = await sharp(uploadCalls[0].buffer).metadata();
    expect(meta.width).toBe(100);
    expect(meta.height).toBe(50);
    expect(meta.format).toBe('png');
    expect(uploadCalls[0].key).toBe('images/t-img/output/photo.png');
    expect(uploadCalls[0].contentType).toBe('image/png');
  });

  it('resizes within bounds when fit=inside', async () => {
    const png = await makeRedPng(800, 200);
    const job = makeJob(png, {
      resize: { width: 400, height: 400, fit: 'inside' },
      format: 'jpeg',
    });

    const result = await processor.process(job);

    // 800x200 inside 400x400 → 400x100
    expect(result.width).toBe(400);
    expect(result.height).toBe(100);
    expect(result.format).toBe('jpeg');

    const meta = await sharp(uploadCalls[0].buffer).metadata();
    expect(meta.format).toBe('jpeg');
    expect(uploadCalls[0].contentType).toBe('image/jpeg');
    expect(uploadCalls[0].key).toBe('images/t-img/output/photo.jpeg');
  });

  it('converts encoding to webp with quality applied', async () => {
    const png = await makeRedPng(200, 200);
    const job = makeJob(png, { format: 'webp', quality: 50 });

    const result = await processor.process(job);

    expect(result.format).toBe('webp');
    const meta = await sharp(uploadCalls[0].buffer).metadata();
    expect(meta.format).toBe('webp');
    expect(uploadCalls[0].contentType).toBe('image/webp');
  });

  it('combines crop + resize + format conversion in one pass', async () => {
    const png = await makeRedPng(1000, 1000);
    const job = makeJob(png, {
      crop: { left: 100, top: 100, width: 800, height: 800 },
      resize: { width: 400, height: 400, fit: 'cover' },
      format: 'avif',
      quality: 60,
    });

    const result = await processor.process(job);

    expect(result.format).toBe('avif');
    expect(result.width).toBe(400);
    expect(result.height).toBe(400);
    const meta = await sharp(uploadCalls[0].buffer).metadata();
    expect(meta.format).toBe('heif');
    expect(uploadCalls[0].key).toBe('images/t-img/output/photo.avif');
  });

  it('defaults to jpeg with output basename when originalFilename missing', async () => {
    const png = await makeRedPng(50, 50);
    getObjectStream.mockResolvedValue(Readable.from(png));
    const job = {
      data: {
        taskId: 't-img',
        source: {
          bucket: 'b',
          key: 'images/t-img/_source/source.bin',
        },
        destination: { bucket: 'b', prefix: 'images/t-img/' },
        options: {},
      },
    } as Job<ImageJobData>;

    await processor.process(job);

    expect(uploadCalls[0].key).toBe('images/t-img/output/output.jpeg');
  });

  it('updates progress on the MediaTask', async () => {
    const png = await makeRedPng(10, 10);
    const job = makeJob(png, {});

    await processor.process(job);

    expect(mediaUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 't-img' },
        data: { progress: { increment: 80 } },
      }),
    );
  });
});
