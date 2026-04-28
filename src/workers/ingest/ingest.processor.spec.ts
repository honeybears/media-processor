import { getFlowProducerToken } from '@nestjs/bullmq';
import { Test } from '@nestjs/testing';
import { Job } from 'bullmq';
import { PrismaService } from '../../prisma/prisma.service';
import {
  FINALIZE_QUEUE,
  IMAGE_QUEUE,
  PIPELINE_FLOW_PRODUCER,
  THUMBNAIL_QUEUE,
  TRANSCODE_1080P_QUEUE,
  TRANSCODE_720P_QUEUE,
} from '../../queue/queue.constants';
import { S3Service } from '../../s3/s3.service';
import { IngestJobData } from '../../pipeline/types';
import { IngestProcessor } from './ingest.processor';

jest.mock('node:fs/promises', () => ({
  mkdir: jest.fn().mockResolvedValue(undefined),
  rm: jest.fn().mockResolvedValue(undefined),
  stat: jest.fn().mockResolvedValue({ size: 2048 }),
}));

jest.mock('node:fs', () => ({
  createWriteStream: jest.fn(() => ({})),
}));

jest.mock('node:stream/promises', () => ({
  pipeline: jest.fn().mockResolvedValue(undefined),
}));

describe('IngestProcessor', () => {
  let processor: IngestProcessor;
  let mediaUpdate: jest.Mock;
  let uploadFile: jest.Mock;
  let flowAdd: jest.Mock;
  let originalFetch: typeof fetch;

  beforeEach(async () => {
    mediaUpdate = jest.fn().mockResolvedValue({ type: 'VIDEO', metadata: {} });
    uploadFile = jest.fn().mockResolvedValue(undefined);
    flowAdd = jest.fn().mockResolvedValue({});

    originalFetch = global.fetch;
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      headers: {
        get: (name: string) =>
          name.toLowerCase() === 'content-type' ? 'video/mp4' : null,
      },
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array([0]));
          controller.close();
        },
      }),
    }) as never;

    const moduleRef = await Test.createTestingModule({
      providers: [
        IngestProcessor,
        {
          provide: PrismaService,
          useValue: { mediaTask: { update: mediaUpdate } },
        },
        { provide: S3Service, useValue: { uploadFile } },
        {
          provide: getFlowProducerToken(PIPELINE_FLOW_PRODUCER),
          useValue: { add: flowAdd },
        },
      ],
    }).compile();

    processor = moduleRef.get(IngestProcessor);
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  it('downloads source, uploads to S3 staging, and enqueues a flow with finalize parent + 3 children for VIDEO', async () => {
    const job = {
      data: {
        taskId: 't-1',
        presignedUrl: 'https://example.com/file.mp4',
        destination: { bucket: 'b', prefix: 'videos/t-1/' },
      },
    } as Job<IngestJobData>;

    const result = await processor.process(job);

    expect(mediaUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 't-1' },
        data: expect.objectContaining({ status: 'PROCESSING' }),
      }),
    );

    expect(uploadFile).toHaveBeenCalledWith(
      expect.objectContaining({
        key: 'videos/t-1/_source/source.bin',
        contentType: 'video/mp4',
      }),
    );

    expect(flowAdd).toHaveBeenCalledTimes(1);
    const tree = flowAdd.mock.calls[0][0] as {
      name: string;
      queueName: string;
      data: { taskId: string };
      children: { queueName: string }[];
    };

    expect(tree.queueName).toBe(FINALIZE_QUEUE);
    expect(tree.data.taskId).toBe('t-1');
    const childQueues = tree.children.map((c) => c.queueName).sort();
    expect(childQueues).toEqual(
      [TRANSCODE_1080P_QUEUE, TRANSCODE_720P_QUEUE, THUMBNAIL_QUEUE].sort(),
    );

    expect(result.source.key).toBe('videos/t-1/_source/source.bin');
    expect(result.source.byteSize).toBe(2048);
  });

  it('enqueues a single image child to IMAGE_QUEUE when task type is IMAGE, propagating imageOptions', async () => {
    mediaUpdate.mockResolvedValue({
      type: 'IMAGE',
      metadata: {
        originalFilename: 'photo.heic',
        imageOptions: {
          crop: { left: 5, top: 5, width: 100, height: 100 },
          format: 'webp',
          quality: 80,
        },
      },
    });

    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      headers: {
        get: (name: string) =>
          name.toLowerCase() === 'content-type' ? 'image/heic' : null,
      },
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array([0]));
          controller.close();
        },
      }),
    });

    const job = {
      data: {
        taskId: 't-img',
        presignedUrl: 'https://example.com/photo.heic',
        destination: { bucket: 'b', prefix: 'images/t-img/' },
      },
    } as Job<IngestJobData>;

    await processor.process(job);

    expect(flowAdd).toHaveBeenCalledTimes(1);
    const tree = flowAdd.mock.calls[0][0] as {
      queueName: string;
      data: { taskId: string };
      children: {
        queueName: string;
        data: { taskId: string; options: unknown };
      }[];
    };
    expect(tree.queueName).toBe(FINALIZE_QUEUE);
    expect(tree.children).toHaveLength(1);
    expect(tree.children[0].queueName).toBe(IMAGE_QUEUE);
    expect(tree.children[0].data.options).toEqual({
      crop: { left: 5, top: 5, width: 100, height: 100 },
      format: 'webp',
      quality: 80,
    });
  });

  it('falls back to empty imageOptions when none stored in metadata', async () => {
    mediaUpdate.mockResolvedValue({ type: 'IMAGE', metadata: {} });

    const job = {
      data: {
        taskId: 't-img2',
        presignedUrl: 'https://example.com/photo.png',
        destination: { bucket: 'b', prefix: 'images/t-img2/' },
      },
    } as Job<IngestJobData>;

    await processor.process(job);

    const tree = flowAdd.mock.calls[0][0] as {
      children: { data: { options: unknown } }[];
    };
    expect(tree.children[0].data.options).toEqual({});
  });

  it('skips fetch and uploads nothing when directUpload is set, but still enqueues children flow', async () => {
    mediaUpdate.mockResolvedValue({
      type: 'VIDEO',
      metadata: { originalFilename: 'movie.mp4' },
    });

    const job = {
      data: {
        taskId: 't-up',
        destination: { bucket: 'b', prefix: 'videos/t-up/' },
        directUpload: { byteSize: 4096, contentType: 'video/mp4' },
      },
    } as Job<IngestJobData>;

    const result = await processor.process(job);

    expect(global.fetch).not.toHaveBeenCalled();
    expect(uploadFile).not.toHaveBeenCalled();

    expect(mediaUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 't-up' },
        data: expect.objectContaining({
          progress: 10,
          sourceKey: 'videos/t-up/_source/source.bin',
          metadata: expect.objectContaining({
            byteSize: 4096,
            contentType: 'video/mp4',
            originalFilename: 'movie.mp4',
          }),
        }),
      }),
    );

    expect(flowAdd).toHaveBeenCalledTimes(1);
    expect(result.source).toEqual(
      expect.objectContaining({
        key: 'videos/t-up/_source/source.bin',
        byteSize: 4096,
        contentType: 'video/mp4',
        originalFilename: 'movie.mp4',
      }),
    );
  });

  it('directUpload + IMAGE fans out to single image child with stored options', async () => {
    mediaUpdate.mockResolvedValue({
      type: 'IMAGE',
      metadata: {
        imageOptions: { format: 'png' },
        originalFilename: 'pic.heic',
      },
    });

    const job = {
      data: {
        taskId: 't-img-up',
        destination: { bucket: 'b', prefix: 'images/t-img-up/' },
        directUpload: { byteSize: 200, contentType: 'image/heic' },
      },
    } as Job<IngestJobData>;

    await processor.process(job);

    const tree = flowAdd.mock.calls[0][0] as {
      queueName: string;
      children: { queueName: string; data: { options: unknown } }[];
    };
    expect(tree.queueName).toBe(FINALIZE_QUEUE);
    expect(tree.children).toHaveLength(1);
    expect(tree.children[0].queueName).toBe(IMAGE_QUEUE);
    expect(tree.children[0].data.options).toEqual({ format: 'png' });
  });

  it('throws when neither presignedUrl nor directUpload is provided', async () => {
    mediaUpdate.mockResolvedValue({ type: 'VIDEO', metadata: {} });

    const job = {
      data: {
        taskId: 't-bad',
        destination: { bucket: 'b', prefix: 'videos/t-bad/' },
      },
    } as Job<IngestJobData>;

    await expect(processor.process(job)).rejects.toThrow(/no source/i);
    expect(flowAdd).not.toHaveBeenCalled();
  });

  it('throws and skips flow enqueue when download fails', async () => {
    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: false,
      status: 502,
      statusText: 'Bad Gateway',
      body: null,
    });

    const job = {
      data: {
        taskId: 't-1',
        presignedUrl: 'https://example.com/file.mp4',
        destination: { bucket: 'b', prefix: 'videos/t-1/' },
      },
    } as Job<IngestJobData>;

    await expect(processor.process(job)).rejects.toThrow(/502/);
    expect(uploadFile).not.toHaveBeenCalled();
    expect(flowAdd).not.toHaveBeenCalled();
  });
});
