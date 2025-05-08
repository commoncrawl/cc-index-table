FROM maven:3.9.9-eclipse-temurin-17-focal AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn package

FROM spark:3.5.5-java17-python3
WORKDIR /app
COPY --from=build /app/target/*.jar ./target/
COPY --from=build /app/src/script/convert_url_index.sh ./src/script/convert_url_index.sh
VOLUME /app/data
ENV SPARK_ON_YARN="--master local"
ENV SPARK_EXTRA_OPTS="--conf spark.executor.userClassPathFirst=true"
ENTRYPOINT ["/app/src/script/convert_url_index.sh"]
