package fr.example.formation.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class SparkStreamJoining {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("CsvSparkStream")
                .master("local[*]")
                .getOrCreate();

        StructType site1Schema = new StructType()
                .add("name", "string")
                .add("production", "integer")
                .add("time", DataTypes.TimestampType);

        Dataset<Row> site1 = spark
                .readStream()
                .option("header", true)
                .option("sep", ";")
                .schema(site1Schema)      // Specify schema of the csv files
                .csv("src/main/resources/java/streaming/join/site1")
                .withColumnRenamed("name", "nameSite1")
                .withColumnRenamed("time", "timeSite1");

        StructType site2Schema = new StructType()
                .add("name", "string")
                .add("pays", "string")
                .add("time", DataTypes.TimestampType);

        Dataset<Row> site2 = spark
                .readStream()
                .option("header", true)
                .option("sep", ";")
                .schema(site2Schema)      // Specify schema of the csv files
                .csv("src/main/resources/java/streaming/join/site2")
                .withWatermark("time", "1 minutes")
                .withColumnRenamed("name", "nameSite2")
                .withColumnRenamed("time", "timeSite2");

        Dataset<Row> tmpJoin =
                site1.join(site2, functions.expr(
                        "nameSite1 = nameSite2 AND " +
                                "timeSite1 >= timeSite2 AND " +
                                "timeSite1 <= timeSite2 + interval 1 hour"));

        if (tmpJoin.isStreaming()) {
            tmpJoin.printSchema();
        }

        StreamingQuery query =
                tmpJoin
                        .writeStream()
                        .outputMode("append")
                        .format("console")
                        .start();

        spark.streams().awaitAnyTermination();
    }
}
