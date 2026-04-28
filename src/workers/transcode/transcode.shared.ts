import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { createWriteStream } from 'node:fs';
import { mkdir, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { PrismaService } from '../../prisma/prisma.service';
import { TranscodeJobData, RenditionResult } from '../../pipeline/types';
import { S3Service } from '../../s3/s3.service';
import { HlsService } from '../../transcoder/hls.service';
import { RenditionPreset } from '../../transcoder/rendition';

const VIDEO_CODECS_BY_HEIGHT: Record<number, string> = {
  1080: 'avc1.640028',
  720: 'avc1.4d401f',
};

const AUDIO_CODEC = 'mp4a.40.2';

export async function runTranscode(params: {
  job: Job<TranscodeJobData>;
  preset: RenditionPreset;
  s3: S3Service;
  hls: HlsService;
  prisma: PrismaService;
  logger: Logger;
}): Promise<RenditionResult> {
  const { job, preset, s3, hls, prisma, logger } = params;
  const { taskId, source, destination } = job.data;
  const workDir = join(tmpdir(), `media-${taskId}-${preset.name}`);
  const inputPath = join(workDir, basename(source.key));
  const outputDir = join(workDir, preset.name);

  await mkdir(workDir, { recursive: true });
  logger.log(`Transcode start task=${taskId} preset=${preset.name}`);

  try {
    const stream = await s3.getObjectStream(source.key);
    await pipeline(stream, createWriteStream(inputPath));
    await stat(inputPath);

    const result = await hls.transcodeHls({
      inputPath,
      outputDir,
      preset: preset,
    });

    const playlistKey = `${destination.prefix}${preset.name}/playlist.m3u8`;
    const segmentKeys: string[] = [];

    for (const segmentPath of result.segmentFiles) {
      const segmentKey = `${destination.prefix}${preset.name}/${basename(segmentPath)}`;
      await s3.uploadFile({
        key: segmentKey,
        filePath: segmentPath,
        contentType: 'video/mp2t',
      });
      segmentKeys.push(segmentKey);
    }

    await s3.uploadFile({
      key: playlistKey,
      filePath: result.playlistFile,
      contentType: 'application/vnd.apple.mpegurl',
    });

    await prisma.mediaTask.update({
      where: { id: taskId },
      data: {
        progress: { increment: 30 },
      },
    });

    logger.log(
      `Transcode done task=${taskId} preset=${preset.name} segments=${segmentKeys.length}`,
    );

    const codecs = `${VIDEO_CODECS_BY_HEIGHT[preset.height] ?? 'avc1.640028'},${AUDIO_CODEC}`;
    const bandwidth =
      result.totalBitrateKbps * 1000 + Math.floor(result.totalBitrateKbps * 50);

    return {
      rendition: preset.name,
      playlistKey,
      segmentKeys,
      width: result.width,
      height: result.height,
      bandwidth,
      codecs,
    };
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}
