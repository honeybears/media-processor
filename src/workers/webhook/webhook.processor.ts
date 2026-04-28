import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Job } from 'bullmq';
import { createHmac } from 'node:crypto';
import { PrismaService } from '../../prisma/prisma.service';
import { WebhookJobData } from '../../pipeline/types';
import { WEBHOOK_QUEUE } from '../../queue/queue.constants';

export class WebhookDeliveryResult {
  status: number;
  attempt: number;
}

@Processor(WEBHOOK_QUEUE)
export class WebhookProcessor extends WorkerHost {
  private readonly logger = new Logger(WebhookProcessor.name);
  private readonly secret: string;

  constructor(
    private readonly prisma: PrismaService,
    config: ConfigService,
  ) {
    super();
    this.secret = config.getOrThrow<string>('WEBHOOK_HMAC_SECRET');
  }

  async process(job: Job<WebhookJobData>): Promise<WebhookDeliveryResult> {
    const { taskId, callbackUrl, payload } = job.data;
    const attempt = job.attemptsMade + 1;
    const body = JSON.stringify(payload);
    const signature = createHmac('sha256', this.secret)
      .update(body)
      .digest('hex');

    this.logger.log(
      `Webhook deliver task=${taskId} attempt=${attempt} url=${callbackUrl}`,
    );

    let response: Response;
    try {
      response = await fetch(callbackUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Webhook-Signature': `sha256=${signature}`,
          'X-Tenant-Id': payload.tenantId,
          'X-Task-Id': payload.taskId,
        },
        body,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.recordAttempt(taskId, attempt);
      throw new Error(`Webhook fetch failed: ${message}`);
    }

    if (!response.ok) {
      await this.recordAttempt(taskId, attempt);
      throw new Error(
        `Webhook responded ${response.status} ${response.statusText}`,
      );
    }

    await this.prisma.mediaTask.update({
      where: { id: taskId },
      data: {
        webhookDeliveredAt: new Date(),
        webhookAttempts: attempt,
      },
    });

    this.logger.log(
      `Webhook delivered task=${taskId} status=${response.status}`,
    );
    return { status: response.status, attempt };
  }

  private async recordAttempt(taskId: string, attempt: number): Promise<void> {
    await this.prisma.mediaTask.update({
      where: { id: taskId },
      data: { webhookAttempts: attempt },
    });
  }
}
