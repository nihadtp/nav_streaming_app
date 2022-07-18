"$SPARK_HOME"/bin/spark-submit \
--class org.mutualFund.App \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
--packages com.typesafe:config:1.3.4,\
net.sourceforge.argparse4j:argparse4j:0.9.0,\
org.apache.kafka:kafka-clients:2.8.0,\
org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
--master "$1" \
 local:///opt/application/prodigal/target/prodigal-1.0-SNAPSHOT.jar \
--config local://prodigal/prodigal_conf.conf;


