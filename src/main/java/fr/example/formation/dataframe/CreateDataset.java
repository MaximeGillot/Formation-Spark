package fr.example.formation.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Concept fondamental : Les Datasets et Dataframes
 * Présentation des différentes méthodes pour créer des Dataframes: à partir de données structurées, de fichiers CSV, de bases de données, etc.
 * Création de Dataframes pour les tests unitaires.
 */
public class CreateDataset {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreation")
                .master("local[*]")
                .getOrCreate();

        // Création de Dataframes pour les tests unitaires
        Dataset<Row> testDataDF = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "John"),
                        RowFactory.create(2, "Alice"),
                        RowFactory.create(3, "Bob")
                ),
                new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty())
                })
        );

        testDataDF.printSchema();
        testDataDF.show(false);

        // Création à partir de fichiers CSV
        Dataset<Row> csvDF =
                spark
                        .read()
                        .option("header", "true")
                        .csv("src/main/resources/java/dataframe/QueryOptimizations/client.csv");
        csvDF.show();

        // Création à partir de bases de données
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/database")
                .option("dbtable", "table")
                .option("user", "username")
                .option("password", "password")
                .load();

        // Création à partir de fichiers Parquet
        String parquetFilePath = "hdfs://path/to/parquet/file.parquet";
        Dataset<Row> parquetDF = spark.read().parquet(parquetFilePath);

        // Création à partir de fichiers ORC
        String orcFilePath = "path/to/orc/file.orc";
        Dataset<Row> orcDF = spark.read().orc(orcFilePath);

        // Création à partir de fichiers JSON
        String jsonFilePath = "path/to/json/file.json";
        Dataset<Row> jsonDF = spark.read().json(jsonFilePath);

        // Fermer la SparkSession
        spark.stop();
    }
}
