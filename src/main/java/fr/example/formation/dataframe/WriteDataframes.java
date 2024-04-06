package fr.example.formation.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Properties;

// https://spark.apache.org/docs/3.0.0/api/java/index.html?org/apache/spark/sql/SaveMode.html
// MongoDB: https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-write/
public class WriteDataframes {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreation")
                .master("local[*]")
                .getOrCreate();

        // Création de Dataframes pour les tests unitaires
        Dataset<Row> testDataDF = spark
                .createDataFrame(
                        List.of(
                                RowFactory.create(1, "John"),
                                RowFactory.create(2, "Alice"),
                                RowFactory.create(3, "Bob"),
                                RowFactory.create(3, "Pierre")
                        ),
                        new StructType(new StructField[]{
                                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("name", DataTypes.StringType, true, Metadata.empty())
                        })
                )
                .repartition(1);

        // Fichier Text
        testDataDF
                .select("name")
                .write()
                .mode(SaveMode.Overwrite)
                .text("src/main/resources/java/dataframe/WriteDataframes/text");

        // Fichier json
        testDataDF
                .write()
                .mode(SaveMode.Append)
                .json("src/main/resources/java/dataframe/WriteDataframes/json");

        // Fichier parquet
        testDataDF
                .write()
                .mode(SaveMode.Ignore)
                .parquet("src/main/resources/java/dataframe/WriteDataframes/parquet");

        // Fichier ORC
        testDataDF
                .write()
                .mode(SaveMode.ErrorIfExists)
                .orc("src/main/resources/java/dataframe/WriteDataframes/orc");

        // Fichier csv
        testDataDF
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("id")
                .csv("src/main/resources/java/dataframe/WriteDataframes/csv");

        // BDD
        testDataDF
                .write()
                .jdbc("jdbc:postgresql://localhost/test?user=fred&password=secret", "client", new Properties());

        // MongoDB
        testDataDF
                .write()
                .format("mongodb")
                .mode(SaveMode.Overwrite)
                .option("database", "people")
                .option("collection", "contacts")
                .save();


        // Fermer la SparkSession
        spark.stop();
    }
}