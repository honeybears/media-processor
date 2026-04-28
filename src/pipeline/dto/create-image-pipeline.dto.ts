import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import {
  IsEnum,
  IsInt,
  IsOptional,
  IsString,
  IsUrl,
  Max,
  Min,
  ValidateNested,
} from 'class-validator';

export class ImageCropDto {
  @IsInt()
  @Min(0)
  @ApiProperty()
  left!: number;

  @IsInt()
  @Min(0)
  @ApiProperty()
  top!: number;

  @IsInt()
  @Min(1)
  @ApiProperty()
  width!: number;

  @IsInt()
  @Min(1)
  @ApiProperty()
  height!: number;
}

export class ImageResizeDto {
  @IsOptional()
  @IsInt()
  @Min(1)
  @ApiPropertyOptional()
  width?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  @ApiPropertyOptional()
  height?: number;

  @IsOptional()
  @IsEnum(['cover', 'contain', 'fill', 'inside', 'outside'])
  @ApiPropertyOptional({
    enum: ['cover', 'contain', 'fill', 'inside', 'outside'],
  })
  fit?: 'cover' | 'contain' | 'fill' | 'inside' | 'outside';
}

export class ImageOptionsDto {
  @IsOptional()
  @ValidateNested()
  @Type(() => ImageCropDto)
  @ApiPropertyOptional({ type: ImageCropDto })
  crop?: ImageCropDto;

  @IsOptional()
  @ValidateNested()
  @Type(() => ImageResizeDto)
  @ApiPropertyOptional({ type: ImageResizeDto })
  resize?: ImageResizeDto;

  @IsOptional()
  @IsEnum(['jpeg', 'png', 'webp', 'avif'])
  @ApiPropertyOptional({ enum: ['jpeg', 'png', 'webp', 'avif'] })
  format?: 'jpeg' | 'png' | 'webp' | 'avif';

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(100)
  @ApiPropertyOptional({ minimum: 1, maximum: 100 })
  quality?: number;
}

export class CreateImagePipelineDto {
  @IsUrl({ require_protocol: true })
  @ApiProperty({ description: 'Presigned URL of the source image' })
  presignedUrl!: string;

  @IsOptional()
  @IsString()
  @ApiPropertyOptional({ description: 'Original filename' })
  originalFilename?: string;

  @IsOptional()
  @IsString()
  @ApiPropertyOptional({ description: 'Tenant identifier' })
  tenantId?: string;

  @IsOptional()
  @IsUrl({ require_protocol: true })
  @ApiPropertyOptional({
    description: 'Webhook URL to POST completion result',
  })
  callbackUrl?: string;

  @IsOptional()
  @ValidateNested()
  @Type(() => ImageOptionsDto)
  @ApiPropertyOptional({ type: ImageOptionsDto })
  options?: ImageOptionsDto;
}
