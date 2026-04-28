import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { PrismaService } from '../../prisma/prisma.service';
import { RenditionResult, TranscodeJobData } from '../../pipeline/types';
import {
  TRANSCODE_1080P_QUEUE,
  TRANSCODE_720P_QUEUE,
} from '../../queue/queue.constants';
import { S3Service } from '../../s3/s3.service';
import { HlsService } from '../../transcoder/hls.service';
import { RENDITIONS, RenditionPreset } from '../../transcoder/rendition';
import { runTranscode } from './transcode.shared';

export abstract class TranscodeProcessorBase extends WorkerHost {
  protected abstract readonly preset: RenditionPreset;
  protected readonly logger = new Logger(this.constructor.name);

  constructor(
    protected readonly prisma: PrismaService,
    protected readonly s3: S3Service,
    protected readonly hls: HlsService,
  ) {
    super();
  }

  process(job: Job<TranscodeJobData>): Promise<RenditionResult> {
    return runTranscode({
      job,
      preset: this.preset,
      s3: this.s3,
      hls: this.hls,
      prisma: this.prisma,
      logger: this.logger,
    });
  }
}

@Processor(TRANSCODE_1080P_QUEUE)
export class Transcode1080pProcessor extends TranscodeProcessorBase {
  protected readonly preset = RENDITIONS['1080p'];

  constructor(prisma: PrismaService, s3: S3Service, hls: HlsService) {
    super(prisma, s3, hls);
  }
}

@Processor(TRANSCODE_720P_QUEUE)
export class Transcode720pProcessor extends TranscodeProcessorBase {
  protected readonly preset = RENDITIONS['720p'];

  constructor(prisma: PrismaService, s3: S3Service, hls: HlsService) {
    super(prisma, s3, hls);
  }
}
