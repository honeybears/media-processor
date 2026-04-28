import {
  BadRequestException,
  Body,
  Controller,
  Post,
  UploadedFile,
  UseInterceptors,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { ApiBody, ApiConsumes, ApiOperation, ApiTags } from '@nestjs/swagger';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { diskStorage } from 'multer';
import { CreateVideoPipelineDto } from './dto/create-video-pipeline.dto';
import { UploadVideoPipelineDto } from './dto/upload-video-pipeline.dto';
import { CreatePipelineResult, PipelineService } from './pipeline.service';

const MAX_VIDEO_UPLOAD_BYTES = 5 * 1024 * 1024 * 1024;

@ApiTags('videos')
@Controller('videos')
export class VideoPipelineController {
  constructor(private readonly pipelineService: PipelineService) {}

  @Post()
  @ApiOperation({ summary: 'Create video processing pipeline (presigned URL)' })
  @ApiBody({ type: CreateVideoPipelineDto })
  create(@Body() dto: CreateVideoPipelineDto): Promise<CreatePipelineResult> {
    return this.pipelineService.createVideoPipeline(dto);
  }

  @Post('upload')
  @ApiOperation({
    summary: 'Upload a video file directly to start the pipeline',
  })
  @ApiConsumes('multipart/form-data')
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: tmpdir(),
        filename: (_req, file, cb) =>
          cb(null, `${randomUUID()}-${file.originalname}`),
      }),
      limits: { fileSize: MAX_VIDEO_UPLOAD_BYTES },
    }),
  )
  upload(
    @UploadedFile() file: Express.Multer.File | undefined,
    @Body() dto: UploadVideoPipelineDto,
  ): Promise<CreatePipelineResult> {
    if (!file) {
      throw new BadRequestException('file field is required');
    }
    return this.pipelineService.uploadVideoPipeline({ file, dto });
  }
}
