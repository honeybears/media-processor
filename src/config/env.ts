import { ConfigService } from '@nestjs/config';

export type WorkerRole =
  | 'api'
  | 'ingest'
  | 'transcode-1080p'
  | 'transcode-720p'
  | 'thumbnail'
  | 'image'
  | 'finalize'
  | 'webhook';

export const WORKER_ROLES: WorkerRole[] = [
  'api',
  'ingest',
  'transcode-1080p',
  'transcode-720p',
  'thumbnail',
  'image',
  'finalize',
  'webhook',
];

export function resolveWorkerRole(): WorkerRole {
  const value = (process.env.WORKER_ROLE ?? 'api') as WorkerRole;
  if (!WORKER_ROLES.includes(value)) {
    throw new Error(
      `Invalid WORKER_ROLE="${value}". Expected one of: ${WORKER_ROLES.join(', ')}`,
    );
  }
  return value;
}

export interface RedisConnectionOptions {
  host: string;
  port: number;
  password?: string;
  username?: string;
  db?: number;
}

export function getRedisConnection(
  config: ConfigService,
): RedisConnectionOptions {
  return {
    host: config.getOrThrow<string>('REDIS_HOST'),
    port: Number(config.getOrThrow<string>('REDIS_PORT')),
    password: config.get<string>('REDIS_PASSWORD') || undefined,
    username: config.get<string>('REDIS_USERNAME') || undefined,
    db: config.get<string>('REDIS_DB')
      ? Number(config.get<string>('REDIS_DB'))
      : undefined,
  };
}
