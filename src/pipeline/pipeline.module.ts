import { Module } from '@nestjs/common';
import { ImagePipelineController } from './image-pipeline.controller';
import { PipelineService } from './pipeline.service';
import { VideoPipelineController } from './video-pipeline.controller';

@Module({
  controllers: [VideoPipelineController, ImagePipelineController],
  providers: [PipelineService],
})
export class PipelineModule {}
