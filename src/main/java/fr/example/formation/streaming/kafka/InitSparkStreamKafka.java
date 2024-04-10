package fr.example.formation.streaming.kafka;

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
 */
public class InitSparkStreamKafka {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("IntroductionSparkStream")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> rawData = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topicTestSpark")
                // .option("subscribe", "topic1,topic2")
                // .option("subscribePattern", "topic.*")
                // .option("startingOffsets", "{\"topic1\":{\"0\":23,\"1\":-2},\"topic2\":{\"0\":-2}}")
                // .option("endingOffsets", "{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}")
                .option("includeHeaders", "true")
                .load();

        StreamingQuery query = rawData.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();
    }
}