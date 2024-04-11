package fr.example.formation.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

/**
 * https://spark.apache.org/docs/latest/streaming-programming-guide.html
 * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
 *
 */
public class CheckpointSparkStream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("CsvSparkStream")
                .master("local[*]")
                .getOrCreate();

        StructType csvSchema = new StructType()
                .add("name", "string")
                .add("age", "integer");

        Dataset<Row> csvDF = spark
                .readStream()
                .option("header", true)
                .option("sep", ";")
                .schema(csvSchema)      // Specify schema of the csv files
                .csv("src/main/resources/java/streaming/csvSparkStream/in");


        if (csvDF.isStreaming()) {
            csvDF.printSchema();
        }

        csvDF.createOrReplaceTempView("updates");

        StreamingQuery query =
                spark
                        .sql("select count(*) as nbLigne from updates")
                        .writeStream()
                        .outputMode("complete")
                        //  .option("checkpointLocation", "src/main/resources/java/streaming/csvSparkStream/checkpoint")
                        .queryName("stream csv")
                        .format("console")
                        .start();


        query.awaitTermination();
    }
}


