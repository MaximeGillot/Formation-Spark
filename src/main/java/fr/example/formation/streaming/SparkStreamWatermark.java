package fr.example.formation.streaming;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

/**
 * Déposer des fichiers (atomique) dans src/main/resources/java/streaming/watermark/in
 */
public class SparkStreamWatermark {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkStreamWatermark")
                .master("local[*]")
                .getOrCreate();

        StructType csvSchema = new StructType()
                .add("name", "string")
                .add("production", "integer")
                .add("time", DataTypes.TimestampType);

        Dataset<Row> csvDF = spark
                .readStream()
                .option("header", true)
                .option("sep", ";")
                .schema(csvSchema)      // Specify schema of the csv files
                .csv("src/main/resources/java/streaming/watermark/in");


        StreamingQuery query =
                csvDF
                        .withWatermark("time", "1 minutes")
                        .groupBy(functions.window(new Column("time"), "1 minutes"))
                        .sum("production")
                        .writeStream()
                        .outputMode("append")
                        .option("truncate", false)
                        .format("console")
                        .start();

        query.awaitTermination();
    }
}
