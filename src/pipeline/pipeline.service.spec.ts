import { getQueueToken } from '@nestjs/bullmq';
import { BadRequestException } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PrismaService } from '../prisma/prisma.service';
import { INGEST_QUEUE } from '../queue/queue.constants';
import { S3Service } from '../s3/s3.service';
import { PipelineService } from './pipeline.service';

function makeMulterFile(
  contents = 'test-bytes',
  mimetype = 'video/mp4',
  originalname = 'sample.mp4',
): Express.Multer.File {
  const dir = mkdtempSync(join(tmpdir(), 'pipeline-spec-'));
  const path = join(dir, originalname);
  writeFileSync(path, contents);
  return {
    fieldname: 'file',
    originalname,
    encoding: '7bit',
    mimetype,
    size: Buffer.byteLength(contents),
    destination: dir,
    filename: originalname,
    path,
    buffer: Buffer.from(''),
    stream: undefined as never,
  };
}

describe('PipelineService', () => {
  let service: PipelineService;
  let prismaCreate: jest.Mock;
  let prismaUpdate: jest.Mock;
  let queueAdd: jest.Mock;
  let s3UploadFile: jest.Mock;

  beforeEach(async () => {
    prismaCreate = jest
      .fn()
      .mockResolvedValue({ id: 't-1', status: 'PENDING' });
    prismaUpdate = jest.fn().mockResolvedValue({});
    queueAdd = jest.fn().mockResolvedValue({});
    s3UploadFile = jest.fn().mockResolvedValue(undefined);

    const moduleRef = await Test.createTestingModule({
      providers: [
        PipelineService,
        {
          provide: PrismaService,
          useValue: {
            mediaTask: { create: prismaCreate, update: prismaUpdate },
          },
        },
        {
          provide: S3Service,
          useValue: { bucket: 'test-bucket', uploadFile: s3UploadFile },
        },
        { provide: getQueueToken(INGEST_QUEUE), useValue: { add: queueAdd } },
      ],
    }).compile();

    service = moduleRef.get(PipelineService);
  });

  describe('createVideoPipeline', () => {
    it('creates a MediaTask, fills destination prefix, and enqueues an ingest job', async () => {
      const result = await service.createVideoPipeline({
        presignedUrl: 'https://example.com/file.mp4',
        originalFilename: 'movie.mp4',
      });

      expect(prismaCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            type: 'VIDEO',
            status: 'PENDING',
            progress: 0,
            sourceBucket: 'test-bucket',
            destinationBucket: 'test-bucket',
            destinationKey: '',
            metadata: expect.objectContaining({
              presignedUrl: 'https://example.com/file.mp4',
              originalFilename: 'movie.mp4',
            }),
          }),
        }),
      );

      expect(prismaUpdate).toHaveBeenCalledWith({
        where: { id: 't-1' },
        data: {
          destinationKey: 'videos/t-1/',
          sourceKey: 'videos/t-1/_source/source.bin',
        },
      });

      expect(queueAdd).toHaveBeenCalledWith(
        'ingest',
        {
          taskId: 't-1',
          presignedUrl: 'https://example.com/file.mp4',
          destination: { bucket: 'test-bucket', prefix: 'videos/t-1/' },
        },
        expect.objectContaining({ attempts: 3 }),
      );

      expect(result).toEqual({ taskId: 't-1', status: 'PENDING' });
    });

    it('stores null originalFilename when not provided', async () => {
      await service.createVideoPipeline({
        presignedUrl: 'https://example.com/file.mp4',
      });

      const data = (
        prismaCreate.mock.calls[0][0] as {
          data: { metadata: { originalFilename: unknown } };
        }
      ).data.metadata.originalFilename;
      expect(data).toBeNull();
    });
  });

  describe('createImagePipeline', () => {
    it('creates an IMAGE MediaTask under images/ prefix with imageOptions persisted', async () => {
      const result = await service.createImagePipeline({
        presignedUrl: 'https://example.com/photo.heic',
        originalFilename: 'photo.heic',
        options: {
          crop: { left: 10, top: 20, width: 300, height: 400 },
          resize: { width: 800, fit: 'inside' },
          format: 'webp',
          quality: 85,
        },
      });

      expect(prismaCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            type: 'IMAGE',
            status: 'PENDING',
            metadata: expect.objectContaining({
              presignedUrl: 'https://example.com/photo.heic',
              originalFilename: 'photo.heic',
              imageOptions: {
                crop: { left: 10, top: 20, width: 300, height: 400 },
                resize: { width: 800, fit: 'inside' },
                format: 'webp',
                quality: 85,
              },
            }),
          }),
        }),
      );

      expect(prismaUpdate).toHaveBeenCalledWith({
        where: { id: 't-1' },
        data: {
          destinationKey: 'images/t-1/',
          sourceKey: 'images/t-1/_source/source.bin',
        },
      });

      expect(queueAdd).toHaveBeenCalledWith(
        'ingest',
        expect.objectContaining({
          taskId: 't-1',
          presignedUrl: 'https://example.com/photo.heic',
          destination: { bucket: 'test-bucket', prefix: 'images/t-1/' },
        }),
        expect.objectContaining({ attempts: 3 }),
      );

      expect(result).toEqual({ taskId: 't-1', status: 'PENDING' });
    });

    it('omits imageOptions from metadata when no options are provided', async () => {
      await service.createImagePipeline({
        presignedUrl: 'https://example.com/photo.png',
      });

      const metadata = (
        prismaCreate.mock.calls[0][0] as {
          data: { metadata: Record<string, unknown> };
        }
      ).data.metadata;
      expect(metadata).not.toHaveProperty('imageOptions');
      expect(metadata.originalFilename).toBeNull();
    });
  });

  describe('uploadVideoPipeline', () => {
    it('uploads file to S3, deletes local copy, and enqueues ingest with directUpload', async () => {
      const file = makeMulterFile('video-bytes', 'video/mp4', 'movie.mp4');

      const result = await service.uploadVideoPipeline({
        file,
        dto: { tenantId: 'tenant-a' },
      });

      expect(s3UploadFile).toHaveBeenCalledWith({
        key: 'videos/t-1/_source/source.bin',
        filePath: file.path,
        contentType: 'video/mp4',
      });

      expect(prismaCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            type: 'VIDEO',
            tenantId: 'tenant-a',
            metadata: expect.objectContaining({
              presignedUrl: null,
              originalFilename: 'movie.mp4',
            }),
          }),
        }),
      );

      expect(queueAdd).toHaveBeenCalledWith(
        'ingest',
        expect.objectContaining({
          taskId: 't-1',
          destination: { bucket: 'test-bucket', prefix: 'videos/t-1/' },
          directUpload: {
            byteSize: Buffer.byteLength('video-bytes'),
            contentType: 'video/mp4',
          },
        }),
        expect.objectContaining({ attempts: 3 }),
      );
      expect(queueAdd.mock.calls[0][1]).not.toHaveProperty('presignedUrl');

      expect(result).toEqual({ taskId: 't-1', status: 'PENDING' });

      // Local upload file should be unlinked. Validate by attempting unlink again.
      await expect(unlink(file.path)).rejects.toThrow();
    });

    it('prefers DTO originalFilename over file.originalname when provided', async () => {
      const file = makeMulterFile('x', 'video/mp4', 'autogen.mp4');
      await service.uploadVideoPipeline({
        file,
        dto: { originalFilename: 'preferred.mp4' },
      });

      const metadata = (
        prismaCreate.mock.calls[0][0] as {
          data: { metadata: Record<string, unknown> };
        }
      ).data.metadata;
      expect(metadata.originalFilename).toBe('preferred.mp4');
    });
  });

  describe('uploadImagePipeline', () => {
    it('parses options JSON and stores it in metadata', async () => {
      const file = makeMulterFile('imgbytes', 'image/png', 'photo.png');

      await service.uploadImagePipeline({
        file,
        dto: {
          options: JSON.stringify({
            format: 'webp',
            quality: 70,
          }),
        },
      });

      expect(s3UploadFile).toHaveBeenCalledWith(
        expect.objectContaining({
          key: 'images/t-1/_source/source.bin',
          contentType: 'image/png',
        }),
      );

      expect(prismaCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            type: 'IMAGE',
            metadata: expect.objectContaining({
              originalFilename: 'photo.png',
              imageOptions: { format: 'webp', quality: 70 },
            }),
          }),
        }),
      );

      const jobData = queueAdd.mock.calls[0][1] as {
        directUpload: { contentType: string };
        destination: { prefix: string };
      };
      expect(jobData.directUpload.contentType).toBe('image/png');
      expect(jobData.destination.prefix).toBe('images/t-1/');
    });

    it('throws BadRequestException for malformed options JSON', async () => {
      const file = makeMulterFile('imgbytes', 'image/png', 'photo.png');

      await expect(
        service.uploadImagePipeline({
          file,
          dto: { options: 'not-json' },
        }),
      ).rejects.toBeInstanceOf(BadRequestException);

      expect(prismaCreate).not.toHaveBeenCalled();
      expect(s3UploadFile).not.toHaveBeenCalled();
      expect(queueAdd).not.toHaveBeenCalled();
    });

    it('omits imageOptions from metadata when options field is absent', async () => {
      const file = makeMulterFile('imgbytes', 'image/png', 'photo.png');
      await service.uploadImagePipeline({ file, dto: {} });

      const metadata = (
        prismaCreate.mock.calls[0][0] as {
          data: { metadata: Record<string, unknown> };
        }
      ).data.metadata;
      expect(metadata).not.toHaveProperty('imageOptions');
    });
  });
});
