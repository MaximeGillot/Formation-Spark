package fr.example.formation.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * https://spark.apache.org/docs/latest/streaming-programming-guide.html
 * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
 * Depuis wsl : nc -lk 9999
 */
public class ManagingSparkStream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("ManagingSparkStream")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();


        // Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .queryName("stream socket port 9999")
                .format("console")
                .start();

        query.processAllAvailable();

        System.out.println("query id: " + query.id()); // get the unique identifier of the running query that persists across restarts from checkpoint data

        System.out.println("Most recent progress: " + query.lastProgress()); // the most recent progress update of this streaming query

        System.out.println("query runId: " + query.runId()); // get the unique id of this run of the query, which will be generated at every start/restart

        System.out.println("query name: " + query.name());  // get the name of the auto-generated or user-specified name

        query.explain();   // print detailed explanations of the query

        query.awaitTermination();   // block until query is terminated, with stop() or with error

        query.stop();      // stop the query

        query.exception();       // the exception if the query has been terminated with error

        query.recentProgress();  // an array of the most recent progress updates for this query
    }
}
