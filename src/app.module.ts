import { DynamicModule, Module, Type } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PipelineModule } from './pipeline/pipeline.module';
import { PrismaModule } from './prisma/prisma.module';
import { QueueModule, QueueModuleOptions } from './queue/queue.module';
import {
  FINALIZE_QUEUE,
  IMAGE_QUEUE,
  INGEST_QUEUE,
  THUMBNAIL_QUEUE,
  TRANSCODE_1080P_QUEUE,
  TRANSCODE_720P_QUEUE,
  WEBHOOK_QUEUE,
} from './queue/queue.constants';
import { S3Module } from './s3/s3.module';
import { TranscoderModule } from './transcoder/transcoder.module';
import { FinalizeModule } from './workers/finalize/finalize.module';
import { ImageModule } from './workers/image/image.module';
import { IngestModule } from './workers/ingest/ingest.module';
import { ThumbnailModule } from './workers/thumbnail/thumbnail.module';
import { TranscodeModule } from './workers/transcode/transcode.module';
import { WebhookModule } from './workers/webhook/webhook.module';
import { WorkerRole } from './config/env';

type ModuleImport = Type<unknown> | DynamicModule;

const ROLE_MODULES: Record<WorkerRole, ModuleImport[]> = {
  api: [PipelineModule],
  ingest: [IngestModule],
  'transcode-1080p': [
    TranscoderModule,
    TranscodeModule.forRole('transcode-1080p'),
  ],
  'transcode-720p': [
    TranscoderModule,
    TranscodeModule.forRole('transcode-720p'),
  ],
  thumbnail: [TranscoderModule, ThumbnailModule],
  image: [ImageModule],
  finalize: [FinalizeModule],
  webhook: [WebhookModule],
};

const ROLE_QUEUES: Record<WorkerRole, QueueModuleOptions> = {
  api: { queues: [INGEST_QUEUE] },
  ingest: { queues: [INGEST_QUEUE], flowProducer: true },
  'transcode-1080p': { queues: [TRANSCODE_1080P_QUEUE] },
  'transcode-720p': { queues: [TRANSCODE_720P_QUEUE] },
  thumbnail: { queues: [THUMBNAIL_QUEUE] },
  image: { queues: [IMAGE_QUEUE] },
  finalize: { queues: [FINALIZE_QUEUE, WEBHOOK_QUEUE] },
  webhook: { queues: [WEBHOOK_QUEUE] },
};

@Module({})
export class AppModule {
  static forRole(role: WorkerRole): DynamicModule {
    return {
      module: AppModule,
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        PrismaModule,
        S3Module,
        QueueModule.forRoot(ROLE_QUEUES[role]),
        ...ROLE_MODULES[role],
      ],
    };
  }
}
