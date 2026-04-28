import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { resolveWorkerRole } from './config/env';
import { DocumentBuilder, OpenAPIObject, SwaggerModule } from '@nestjs/swagger';

async function bootstrap(): Promise<void> {
  const role = resolveWorkerRole();
  const logger = new Logger('Bootstrap');

  const app = await NestFactory.create(AppModule.forRole(role));
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));

  const config = new DocumentBuilder()
    .setTitle('Media Processor')
    .setDescription('Media Processing API')
    .setVersion('1.0')
    .addTag('media')
    .build();
  const documentFactory = (): OpenAPIObject =>
    SwaggerModule.createDocument(app, config);

  if (role === 'api') {
    const port = Number(process.env.PORT ?? 3000);
    SwaggerModule.setup('api', app, documentFactory());
    await app.listen(port);
    logger.log(`API listening on :${port}`);
  } else {
    await app.init();
    logger.log(`Worker started role=${role}`);
  }
}

void bootstrap();
