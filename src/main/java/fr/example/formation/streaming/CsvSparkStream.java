package fr.example.formation.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

/**
 * Déposer des fichiers (atomique) dans src/main/resources/java/streaming/csvSparkStream/in
 */
public class CsvSparkStream {
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
                .format("console")
                .start();


        query.awaitTermination();
    }
}
