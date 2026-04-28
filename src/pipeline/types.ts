export interface PipelineSource {
  bucket: string;
  key: string;
  contentType?: string;
  byteSize?: number;
  originalFilename?: string;
}

export interface PipelineDestination {
  bucket: string;
  prefix: string;
}

export interface DirectUploadDescriptor {
  byteSize: number;
  contentType: string;
}

export interface IngestJobData {
  taskId: string;
  presignedUrl?: string;
  directUpload?: DirectUploadDescriptor;
  destination: PipelineDestination;
}

export interface TranscodeJobData {
  taskId: string;
  source: PipelineSource;
  destination: PipelineDestination;
}

export type ThumbnailJobData = TranscodeJobData;

export type ImageOutputFormat = 'jpeg' | 'png' | 'webp' | 'avif';

export interface ImageCrop {
  left: number;
  top: number;
  width: number;
  height: number;
}

export interface ImageResize {
  width?: number;
  height?: number;
  fit?: 'cover' | 'contain' | 'fill' | 'inside' | 'outside';
}

export interface ImageOptions {
  crop?: ImageCrop;
  resize?: ImageResize;
  format?: ImageOutputFormat;
  quality?: number;
}

export interface ImageJobData {
  taskId: string;
  source: PipelineSource;
  destination: PipelineDestination;
  options: ImageOptions;
}

export interface FinalizeJobData {
  taskId: string;
  destination: PipelineDestination;
  source: PipelineSource;
}

export class RenditionResult {
  rendition: '1080p' | '720p';
  playlistKey: string;
  segmentKeys: string[];
  width: number;
  height: number;
  bandwidth: number;
  codecs: string;
}

export class ThumbnailResult {
  type: 'thumbnail';
  lowResKey: string;
  highResKey: string;
}

export class ImageResult {
  type: 'image';
  outputKey: string;
  format: ImageOutputFormat;
  width: number;
  height: number;
  byteSize: number;
}

export interface WebhookRendition {
  name: '1080p' | '720p';
  playlistKey: string;
  width: number;
  height: number;
  bandwidth: number;
}

export interface WebhookImage {
  outputKey: string;
  format: ImageOutputFormat;
  width: number;
  height: number;
  byteSize: number;
}

export interface WebhookPayload {
  taskId: string;
  tenantId: string;
  status: 'COMPLETED' | 'FAILED';
  bucket: string;
  masterKey?: string;
  renditions?: WebhookRendition[];
  image?: WebhookImage;
  error?: string;
  finishedAt: string;
}

export interface WebhookJobData {
  taskId: string;
  callbackUrl: string;
  payload: WebhookPayload;
}
