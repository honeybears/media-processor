export interface RenditionPreset {
  name: '1080p' | '720p';
  height: number;
  videoBitrateKbps: number;
  maxrateKbps: number;
  bufsizeKbps: number;
  audioBitrateKbps: number;
}

export const RENDITIONS: Record<RenditionPreset['name'], RenditionPreset> = {
  '1080p': {
    name: '1080p',
    height: 1080,
    videoBitrateKbps: 5000,
    maxrateKbps: 5350,
    bufsizeKbps: 7500,
    audioBitrateKbps: 192,
  },
  '720p': {
    name: '720p',
    height: 720,
    videoBitrateKbps: 2800,
    maxrateKbps: 2996,
    bufsizeKbps: 4200,
    audioBitrateKbps: 128,
  },
};
