import { DynamicModule, Module, Type } from '@nestjs/common';
import {
  Transcode1080pProcessor,
  Transcode720pProcessor,
  TranscodeProcessorBase,
} from './transcode.processor';

export type TranscodeRole = 'transcode-1080p' | 'transcode-720p';

const PROCESSOR_BY_ROLE: Record<TranscodeRole, Type<TranscodeProcessorBase>> = {
  'transcode-1080p': Transcode1080pProcessor,
  'transcode-720p': Transcode720pProcessor,
};

@Module({})
export class TranscodeModule {
  static forRole(role: TranscodeRole): DynamicModule {
    return {
      module: TranscodeModule,
      providers: [PROCESSOR_BY_ROLE[role]],
    };
  }
}
