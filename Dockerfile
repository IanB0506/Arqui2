# -----------------------------------------------------------------------------
# Imagen comun para los 4 microservicios. Se elige el modulo con --build-arg MODULE=...
#
#   target "runtime"  (por defecto): compila con Maven DENTRO de Docker. Solo requiere Docker.
#   target "prebuilt": usa el jar ya compilado en <modulo>/target/app.jar (mvn package previo).
#                      Mas rapido y sin internet durante el build (util en el laboratorio).
# -----------------------------------------------------------------------------
ARG MODULE=alert-service

FROM maven:3.9.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:21-jre AS base
RUN groupadd --system centinela && useradd --system --gid centinela --home /app centinela \
    && mkdir -p /app /data/spool && chown -R centinela:centinela /app /data
WORKDIR /app
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError" TZ=America/Guatemala
USER centinela
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]

FROM base AS prebuilt
ARG MODULE
COPY --chown=centinela:centinela ${MODULE}/target/app.jar /app/app.jar

FROM base AS runtime
ARG MODULE
COPY --from=build --chown=centinela:centinela /src/${MODULE}/target/app.jar /app/app.jar
