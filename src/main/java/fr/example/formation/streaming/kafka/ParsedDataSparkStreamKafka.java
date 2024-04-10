package fr.example.formation.streaming.kafka;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

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

        StructType clientSchema = new StructType()
                .add("name", "string")
                .add("age", "string");

        // binaire -> string -> json struct -> flatten
        Dataset<Row> parsedData =
                rawData
                        .withColumn("json", new Column("value").cast("string")) // cast du binaire en string
                        .withColumn("data", functions.from_json(new Column("json"), clientSchema)) // cast du string en json (struct)
                        .select("*", "data.*"); // flatten structure

        StreamingQuery query = parsedData.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start();

        query.awaitTermination();
    }
}
