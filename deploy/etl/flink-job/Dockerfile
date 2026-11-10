ARG FLINK_VERSION=1.20.1-scala_2.12-java17
ARG FLINK_ARTIFACT_VERSION=1.20.1

FROM maven:3.9.9-eclipse-temurin-17 AS build
ARG FLINK_ARTIFACT_VERSION
WORKDIR /build
COPY pom.xml ./
RUN mvn -B -DskipTests dependency:go-offline \
    && mvn -B dependency:copy \
      -Dartifact=org.apache.flink:flink-statebackend-rocksdb:${FLINK_ARTIFACT_VERSION} \
      -DoutputDirectory=/build/statebackend
COPY src ./src
RUN mvn -B verify

FROM flink:${FLINK_VERSION}
USER root
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop /opt/flink/usrlib \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/
COPY --from=build /build/target/aiopslab-telemetry-etl.jar /opt/flink/usrlib/aiopslab-telemetry-etl.jar
COPY --from=build /build/statebackend/ /opt/flink/lib/
RUN chown -R flink:flink /opt/flink/plugins/s3-fs-hadoop /opt/flink/usrlib
USER flink
