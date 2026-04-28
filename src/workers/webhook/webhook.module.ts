import { Module } from '@nestjs/common';
import { WebhookProcessor } from './webhook.processor';

@Module({
  providers: [WebhookProcessor],
})
export class WebhookModule {}
