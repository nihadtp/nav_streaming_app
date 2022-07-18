FROM maven:3.8.2-eclipse-temurin-17 AS build
WORKDIR /opt/application
COPY . prodigal
RUN mvn -f prodigal/pom.xml clean package

FROM datamechanics/spark:3.2.0-hadoop-3.3.1-java-8-scala-2.12-python-3.8-latest
WORKDIR /opt/application
COPY . prodigal
ENV SPARK_USER=spark3
COPY --from=build /opt/application/prodigal/target prodigal/target
#ENTRYPOINT ["bash", "prodigal/spark_k.sh"]

