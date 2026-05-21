# Base test image: Maven 3.9 + JDK 17 + samtools built from source.
# Used by: `docker compose run spark-test mvn test`
FROM maven:3.9-eclipse-temurin-17

# samtools 1.21 is the last version confirmed at repo creation time.
# Override at build time with: --build-arg SAMTOOLS_VERSION=1.21
ARG SAMTOOLS_VERSION=1.21

# Build deps for samtools + curl for the download
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl bzip2 make gcc \
        zlib1g-dev libbz2-dev liblzma-dev \
        libcurl4-openssl-dev libssl-dev libncurses5-dev \
    && rm -rf /var/lib/apt/lists/*

COPY docker/samtools-build.sh /tmp/samtools-build.sh
RUN chmod +x /tmp/samtools-build.sh \
    && SAMTOOLS_VERSION="${SAMTOOLS_VERSION}" /tmp/samtools-build.sh \
    && rm /tmp/samtools-build.sh

# Spark 4.x on Java 17 requires these module opens for internal reflection.
# Set here so they apply to both `mvn test` and any direct java invocations.
ENV MAVEN_OPTS="\
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -Xmx2g"

WORKDIR /workspace
