import { Module } from '@nestjs/common';
import { IngestProcessor } from './ingest.processor';

@Module({
  providers: [IngestProcessor],
})
export class IngestModule {}
