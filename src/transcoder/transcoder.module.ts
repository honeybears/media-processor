import { Global, Module } from '@nestjs/common';
import { HlsService } from './hls.service';

@Global()
@Module({
  providers: [HlsService],
  exports: [HlsService],
})
export class TranscoderModule {}
