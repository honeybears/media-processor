export const INGEST_QUEUE = 'ingest';
export const TRANSCODE_1080P_QUEUE = 'transcode-1080p';
export const TRANSCODE_720P_QUEUE = 'transcode-720p';
export const THUMBNAIL_QUEUE = 'thumbnail';
export const IMAGE_QUEUE = 'image';
export const FINALIZE_QUEUE = 'finalize';
export const WEBHOOK_QUEUE = 'webhook';

export const PIPELINE_FLOW_PRODUCER = 'pipeline';

export const ALL_QUEUES = [
  INGEST_QUEUE,
  TRANSCODE_1080P_QUEUE,
  TRANSCODE_720P_QUEUE,
  THUMBNAIL_QUEUE,
  IMAGE_QUEUE,
  FINALIZE_QUEUE,
  WEBHOOK_QUEUE,
] as const;
