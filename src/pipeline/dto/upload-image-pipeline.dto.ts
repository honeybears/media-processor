import { ApiPropertyOptional } from '@nestjs/swagger';
import { IsOptional, IsString, IsUrl } from 'class-validator';

export class UploadImagePipelineDto {
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
  @ApiPropertyOptional({ description: 'Webhook URL to POST completion result' })
  callbackUrl?: string;

  @IsOptional()
  @IsString()
  @ApiPropertyOptional({
    description:
      'JSON-encoded ImageOptions (crop / resize / format / quality). Example: {"format":"webp","quality":85}',
  })
  options?: string;
}
