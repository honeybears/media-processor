import { BullModule } from '@nestjs/bullmq';
import { DynamicModule, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { getRedisConnection } from '../config/env';
import { PIPELINE_FLOW_PRODUCER } from './queue.constants';

export interface QueueModuleOptions {
  queues: string[];
  flowProducer?: boolean;
}

@Module({})
export class QueueModule {
  static forRoot(options: QueueModuleOptions): DynamicModule {
    const bullRoot = BullModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        connection: getRedisConnection(config),
      }),
    });

    const queueRegistrations =
      options.queues.length > 0
        ? [
            BullModule.registerQueue(
              ...options.queues.map((name) => ({ name })),
            ),
          ]
        : [];

    const flowRegistration = options.flowProducer
      ? [
          BullModule.registerFlowProducer({
            name: PIPELINE_FLOW_PRODUCER,
          }),
        ]
      : [];

    const imports = [bullRoot, ...queueRegistrations, ...flowRegistration];

    return {
      module: QueueModule,
      imports,
      exports: imports,
      global: true,
    };
  }
}
