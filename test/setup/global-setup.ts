import { CreateBucketCommand, S3Client } from '@aws-sdk/client-s3';
import { execSync } from 'node:child_process';
import { GenericContainer, Wait } from 'testcontainers';

export default async function globalSetup() {
  const log = (msg: string) => console.log(`[e2e:setup] ${msg}`);

  log('Starting postgres container...');
  const pg = await new GenericContainer('postgres:16-alpine')
    .withEnvironment({
      POSTGRES_USER: 'test',
      POSTGRES_PASSWORD: 'test',
      POSTGRES_DB: 'test',
    })
    .withExposedPorts(5432)
    .withWaitStrategy(
      Wait.forLogMessage(/database system is ready to accept connections/, 2),
    )
    .start();

  log('Starting redis container...');
  const redis = await new GenericContainer('redis:7-alpine')
    .withExposedPorts(6379)
    .withWaitStrategy(Wait.forLogMessage('Ready to accept connections'))
    .start();

  log('Starting minio container...');
  const minio = await new GenericContainer('minio/minio:latest')
    .withCommand(['server', '/data'])
    .withEnvironment({
      MINIO_ROOT_USER: 'minioadmin',
      MINIO_ROOT_PASSWORD: 'minioadmin',
    })
    .withExposedPorts(9000)
    .withWaitStrategy(Wait.forHttp('/minio/health/live', 9000))
    .start();

  const databaseUrl = `postgresql://test:test@${pg.getHost()}:${pg.getMappedPort(5432)}/test?schema=public`;
  const s3Endpoint = `http://${minio.getHost()}:${minio.getMappedPort(9000)}`;

  process.env.DATABASE_URL = databaseUrl;
  process.env.REDIS_HOST = redis.getHost();
  process.env.REDIS_PORT = String(redis.getMappedPort(6379));
  process.env.S3_ENDPOINT = s3Endpoint;
  process.env.S3_BUCKET = 'test-bucket';
  process.env.S3_FORCE_PATH_STYLE = 'true';
  process.env.AWS_REGION = 'us-east-1';
  process.env.AWS_ACCESS_KEY_ID = 'minioadmin';
  process.env.AWS_SECRET_ACCESS_KEY = 'minioadmin';

  log('Pushing prisma schema to test database...');
  execSync('npx prisma db push --accept-data-loss', {
    stdio: 'inherit',
    env: process.env,
  });

  log('Creating test bucket on MinIO...');
  const s3 = new S3Client({
    endpoint: s3Endpoint,
    region: 'us-east-1',
    forcePathStyle: true,
    credentials: {
      accessKeyId: 'minioadmin',
      secretAccessKey: 'minioadmin',
    },
  });
  await s3.send(new CreateBucketCommand({ Bucket: 'test-bucket' }));
  s3.destroy();

  (globalThis as Record<string, unknown>).__E2E_CONTAINERS__ = {
    pg,
    redis,
    minio,
  };

  log('Containers ready');
}
