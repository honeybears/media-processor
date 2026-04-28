import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { Readable } from 'node:stream';
import { TranscodeJobData } from '../../pipeline/types';
import { RENDITIONS } from '../../transcoder/rendition';
import { runTranscode } from './transcode.shared';

jest.mock('node:fs/promises', () => ({
  mkdir: jest.fn().mockResolvedValue(undefined),
  rm: jest.fn().mockResolvedValue(undefined),
  stat: jest.fn().mockResolvedValue({ size: 1024 }),
}));

jest.mock('node:fs', () => ({
  createWriteStream: jest.fn(() => ({})),
}));

jest.mock('node:stream/promises', () => ({
  pipeline: jest.fn().mockResolvedValue(undefined),
}));

describe('runTranscode', () => {
  let s3: { getObjectStream: jest.Mock; uploadFile: jest.Mock };
  let hls: { transcodeHls: jest.Mock };
  let prisma: { mediaTask: { update: jest.Mock } };

  beforeEach(() => {
    s3 = {
      getObjectStream: jest.fn().mockResolvedValue(Readable.from([])),
      uploadFile: jest.fn().mockResolvedValue(undefined),
    };
    hls = {
      transcodeHls: jest.fn().mockResolvedValue({
        playlistFile: '/tmp/work/720p/playlist.m3u8',
        segmentFiles: [
          '/tmp/work/720p/segment_001.ts',
          '/tmp/work/720p/segment_002.ts',
          '/tmp/work/720p/segment_003.ts',
        ],
        width: 1280,
        height: 720,
        totalBitrateKbps:
          RENDITIONS['720p'].videoBitrateKbps +
          RENDITIONS['720p'].audioBitrateKbps,
      }),
    };
    prisma = {
      mediaTask: { update: jest.fn().mockResolvedValue({}) },
    };
  });

  it('uploads each segment and the playlist, returns rendition metadata', async () => {
    const job = {
      data: {
        taskId: 't-1',
        source: { bucket: 'b', key: 'videos/t-1/_source/source.bin' },
        destination: { bucket: 'b', prefix: 'videos/t-1/' },
      },
    } as Job<TranscodeJobData>;

    const result = await runTranscode({
      job,
      preset: RENDITIONS['720p'],
      s3: s3 as never,
      hls: hls as never,
      prisma: prisma as never,
      logger: new Logger('test'),
    });

    expect(s3.uploadFile).toHaveBeenCalledTimes(4);
    expect(s3.uploadFile).toHaveBeenCalledWith(
      expect.objectContaining({
        key: 'videos/t-1/720p/segment_001.ts',
        contentType: 'video/mp2t',
      }),
    );
    expect(s3.uploadFile).toHaveBeenCalledWith(
      expect.objectContaining({
        key: 'videos/t-1/720p/segment_003.ts',
      }),
    );
    expect(s3.uploadFile).toHaveBeenCalledWith(
      expect.objectContaining({
        key: 'videos/t-1/720p/playlist.m3u8',
        contentType: 'application/vnd.apple.mpegurl',
      }),
    );

    expect(prisma.mediaTask.update).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: 't-1' },
        data: { progress: { increment: 30 } },
      }),
    );

    expect(result.rendition).toBe('720p');
    expect(result.height).toBe(720);
    expect(result.playlistKey).toBe('videos/t-1/720p/playlist.m3u8');
    expect(result.segmentKeys).toEqual([
      'videos/t-1/720p/segment_001.ts',
      'videos/t-1/720p/segment_002.ts',
      'videos/t-1/720p/segment_003.ts',
    ]);
    expect(result.codecs).toContain('mp4a.40.2');
  });
});
