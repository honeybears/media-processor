import { StartedTestContainer } from 'testcontainers';

export default async function globalTeardown() {
  const containers = (globalThis as Record<string, unknown>)
    .__E2E_CONTAINERS__ as
    | {
        pg: StartedTestContainer;
        redis: StartedTestContainer;
        minio: StartedTestContainer;
      }
    | undefined;

  if (!containers) return;

  await Promise.allSettled([
    containers.pg.stop(),
    containers.redis.stop(),
    containers.minio.stop(),
  ]);
}
