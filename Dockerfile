FROM maven:3.6.3-jdk-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn package

FROM spark:3.5.1
WORKDIR /app
COPY --from=build /app/target/*.jar ./target/
COPY --from=build /app/src/script/convert_url_index.sh ./src/script/convert_url_index.sh
VOLUME /app/data
ENV SPARK_ON_YARN="--master local"
ENTRYPOINT ["/app/src/script/convert_url_index.sh"]