docker run --name spark -p 9092:9090 -p 4042:4040 nihadtp/mutual-fund-streaming:v1 driver \
--class org.mutualFund.App \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
--conf spark.executor.instances=3 \
--conf spark.kubernetes.container.image=nihadtp/mutual-fund-streaming:v1 \
--packages com.typesafe:config:1.3.4,\
net.sourceforge.argparse4j:argparse4j:0.9.0,\
org.apache.kafka:kafka-clients:2.8.0,\
org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
--master k8s://https://127.0.0.1:62704 \
 local:///opt/application/prodigal/target/prodigal-1.0-SNAPSHOT.jar \
--config /opt/application/prodigal/prodigal_conf.conf;


