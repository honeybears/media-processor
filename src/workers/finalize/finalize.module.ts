import { Module } from '@nestjs/common';
import { FinalizeProcessor } from './finalize.processor';

@Module({
  providers: [FinalizeProcessor],
})
export class FinalizeModule {}
