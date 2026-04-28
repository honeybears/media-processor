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
import { CreateImagePipelineDto } from './dto/create-image-pipeline.dto';
import { UploadImagePipelineDto } from './dto/upload-image-pipeline.dto';
import { CreatePipelineResult, PipelineService } from './pipeline.service';

const MAX_IMAGE_UPLOAD_BYTES = 100 * 1024 * 1024;

@ApiTags('images')
@Controller('images')
export class ImagePipelineController {
  constructor(private readonly pipelineService: PipelineService) {}

  @Post()
  @ApiOperation({ summary: 'Create image processing pipeline (presigned URL)' })
  @ApiBody({ type: CreateImagePipelineDto })
  create(@Body() dto: CreateImagePipelineDto): Promise<CreatePipelineResult> {
    return this.pipelineService.createImagePipeline(dto);
  }

  @Post('upload')
  @ApiOperation({
    summary: 'Upload an image file directly to start the pipeline',
  })
  @ApiConsumes('multipart/form-data')
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: tmpdir(),
        filename: (_req, file, cb) =>
          cb(null, `${randomUUID()}-${file.originalname}`),
      }),
      limits: { fileSize: MAX_IMAGE_UPLOAD_BYTES },
    }),
  )
  upload(
    @UploadedFile() file: Express.Multer.File | undefined,
    @Body() dto: UploadImagePipelineDto,
  ): Promise<CreatePipelineResult> {
    if (!file) {
      throw new BadRequestException('file field is required');
    }
    return this.pipelineService.uploadImagePipeline({ file, dto });
  }
}
