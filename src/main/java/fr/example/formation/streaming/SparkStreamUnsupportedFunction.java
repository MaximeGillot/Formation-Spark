package fr.example.formation.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * <a href="https://spark.apache.org/docs/latest/streaming-programming-guide.html">...</a>
 * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html">...</a>
 * Depuis wsl : nc -lk 9999
 */
public class SparkStreamUnsupportedFunction {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("IntroductionSparkStream")
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


        /*
         * org.apache.spark.sql.AnalysisException: Complete output mode not supported
         * when there are no streaming aggregations on streaming DataFrames/Datasets;
         */

        // words = words.distinct();

       /* StreamingQuery query = words.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();*/


        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // limit + update = unsupported
        // wordCounts = wordCounts.limit(1);

        /*
         * Exception in thread "main" org.apache.spark.sql.AnalysisException:
         * Queries with streaming sources must be executed with writeStream.start();
         * foreach() - Instead use ds.writeStream.foreach(...)
         */
        // wordCounts.foreach((ForeachFunction<Row>) System.out::println);

        /*
         *Exception in thread "main" org.apache.spark.sql.AnalysisException:
         *  Queries with streaming sources must be executed with writeStream.start();
         *  show() - Instead use the console sink
         */
        // wordCounts.show();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();


        query.awaitTermination();
    }
}
