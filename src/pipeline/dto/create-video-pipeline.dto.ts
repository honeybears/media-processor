import { ApiProperty } from '@nestjs/swagger';
import { IsOptional, IsString, IsUrl } from 'class-validator';

export class CreateVideoPipelineDto {
  @IsUrl({ require_protocol: true })
  @ApiProperty({ description: 'Presigned URL' })
  presignedUrl!: string;

  @IsOptional()
  @IsString()
  @ApiProperty({ description: 'Original filename', required: false })
  originalFilename?: string;

  @IsOptional()
  @IsString()
  @ApiProperty({
    description: 'Tenant identifier for multi-tenant scoping',
    required: false,
  })
  tenantId?: string;

  @IsOptional()
  @IsUrl({ require_protocol: true })
  @ApiProperty({
    description:
      'Webhook URL to POST completion result. HMAC-SHA256 signature in X-Webhook-Signature header.',
    required: false,
  })
  callbackUrl?: string;
}
