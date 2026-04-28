import { Injectable, Logger } from '@nestjs/common';
import ffmpeg from 'fluent-ffmpeg';
import { mkdir, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { RenditionPreset } from './rendition';

export interface HlsResult {
  playlistFile: string;
  segmentFiles: string[];
  width: number;
  height: number;
  totalBitrateKbps: number;
}

export interface ProbeResult {
  width: number;
  height: number;
  durationSec: number;
}

@Injectable()
export class HlsService {
  private readonly logger = new Logger(HlsService.name);

  probe(filePath: string): Promise<ProbeResult> {
    return new Promise((resolve, reject) => {
      ffmpeg.ffprobe(filePath, (err, data) => {
        if (err) {
          return reject(err instanceof Error ? err : new Error(String(err)));
        }
        const stream = (data.streams ?? []).find(
          (s) => s.codec_type === 'video',
        );
        if (!stream) {
          return reject(new Error('No video stream found'));
        }
        resolve({
          width: stream.width ?? 0,
          height: stream.height ?? 0,
          durationSec: Number(data.format?.duration ?? 0),
        });
      });
    });
  }

  async transcodeHls(params: {
    inputPath: string;
    outputDir: string;
    preset: RenditionPreset;
  }): Promise<HlsResult> {
    const { inputPath, outputDir, preset } = params;
    await mkdir(outputDir, { recursive: true });

    const playlistFile = join(outputDir, 'playlist.m3u8');
    const segmentPattern = join(outputDir, 'segment_%03d.ts');

    await new Promise<void>((resolve, reject) => {
      ffmpeg(inputPath)
        .outputOptions([
          '-c:v libx264',
          '-preset veryfast',
          '-profile:v main',
          `-vf scale=-2:${preset.height}`,
          `-b:v ${preset.videoBitrateKbps}k`,
          `-maxrate ${preset.maxrateKbps}k`,
          `-bufsize ${preset.bufsizeKbps}k`,
          '-g 48',
          '-keyint_min 48',
          '-sc_threshold 0',
          '-c:a aac',
          `-b:a ${preset.audioBitrateKbps}k`,
          '-ac 2',
          '-ar 48000',
          '-hls_time 6',
          '-hls_playlist_type vod',
          '-hls_flags independent_segments',
          `-hls_segment_filename ${segmentPattern}`,
        ])
        .output(playlistFile)
        .on('start', (cmd) => this.logger.debug(`ffmpeg start: ${cmd}`))
        .on('error', (err) => reject(err))
        .on('end', () => resolve())
        .run();
    });

    const files = await readdir(outputDir);
    const segmentFiles = files
      .filter((f) => f.endsWith('.ts'))
      .sort()
      .map((f) => join(outputDir, f));

    const probe = await this.probe(inputPath);
    const widthForHeight = Math.round(
      (probe.width / probe.height) * preset.height,
    );
    const evenWidth = widthForHeight - (widthForHeight % 2);

    return {
      playlistFile,
      segmentFiles,
      width: evenWidth,
      height: preset.height,
      totalBitrateKbps: preset.videoBitrateKbps + preset.audioBitrateKbps,
    };
  }

  async extractThumbnail(params: {
    inputPath: string;
    outputPath: string;
    width: number;
    timestampSec: number;
  }): Promise<void> {
    const { inputPath, outputPath, width, timestampSec } = params;
    await new Promise<void>((resolve, reject) => {
      ffmpeg(inputPath)
        .seekInput(timestampSec)
        .outputOptions(['-frames:v 1', '-q:v 2', `-vf scale=${width}:-2`])
        .output(outputPath)
        .on('error', (err) => reject(err))
        .on('end', () => resolve())
        .run();
    });
  }
}
