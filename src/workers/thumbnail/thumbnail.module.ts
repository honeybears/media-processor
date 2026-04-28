import { Module } from '@nestjs/common';
import { ThumbnailProcessor } from './thumbnail.processor';

@Module({
  providers: [ThumbnailProcessor],
})
export class ThumbnailModule {}
