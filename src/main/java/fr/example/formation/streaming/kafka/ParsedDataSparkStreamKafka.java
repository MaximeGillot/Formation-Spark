package fr.example.formation.streaming.kafka;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

/**
 * https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 * Ajouter la dependence kafka dans le pom
 * kafka 0.10 minimum
 * <p>
 * Créer un console producer: sh bin/kafka-server-start.sh config/server.properties
 * ./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topicTestSpark --property "parse.key=true" --property "key.separator=;"
 * message : key;{"name":"maxime","age":"24"}
 */
public class ParsedDataSparkStreamKafka {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("IntroductionSparkStream")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> rawData = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topicTestSpark")
                .option("includeHeaders", "true")
                .load();

        Dataset<Row> parsedData =
                rawData
                        .withColumn("keyString", new Column("key").cast("string")) // cast du binaire en string
                        .withColumn("valueString", new Column("value").cast("string")); // cast du binaire en string

        StreamingQuery query = parsedData.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        query.awaitTermination();
    }
}
