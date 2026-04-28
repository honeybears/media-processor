import { Module } from '@nestjs/common';
import { ImageProcessor } from './image.processor';

@Module({
  providers: [ImageProcessor],
})
export class ImageModule {}
