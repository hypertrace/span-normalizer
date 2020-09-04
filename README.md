# Span Normalizer

Converts the incoming spans from jaeger or any other format to a raw span format which is understood by the rest of the Hypertrace platform.

## Building locally
The Span normalizer uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Span normalizer, run:

```
./gradlew dockerBuildImages
```

## Docker Image Source:
- [DockerHub > Span normalizer](https://hub.docker.com/r/hypertrace/span-normalizer)
