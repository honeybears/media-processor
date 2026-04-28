import {
  GetObjectCommand,
  HeadObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { ConnectionOptions } from 'bullmq';

export function getRedisConnection(): ConnectionOptions {
  return {
    host: process.env.REDIS_HOST,
    port: Number(process.env.REDIS_PORT),
    maxRetriesPerRequest: null,
  };
}

export function makeS3Client(): S3Client {
  return new S3Client({
    endpoint: process.env.S3_ENDPOINT,
    region: process.env.AWS_REGION,
    forcePathStyle: true,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
    },
  });
}

export async function s3ObjectExists(
  client: S3Client,
  key: string,
): Promise<boolean> {
  try {
    await client.send(
      new HeadObjectCommand({ Bucket: process.env.S3_BUCKET, Key: key }),
    );
    return true;
  } catch {
    return false;
  }
}

export async function s3HeadSize(
  client: S3Client,
  key: string,
): Promise<number> {
  const head = await client.send(
    new HeadObjectCommand({ Bucket: process.env.S3_BUCKET, Key: key }),
  );
  return head.ContentLength ?? 0;
}

export async function s3GetText(
  client: S3Client,
  key: string,
): Promise<string> {
  const result = await client.send(
    new GetObjectCommand({ Bucket: process.env.S3_BUCKET!, Key: key }),
  );
  if (!result.Body) throw new Error(`Missing body for key=${key}`);
  return result.Body.transformToString();
}
