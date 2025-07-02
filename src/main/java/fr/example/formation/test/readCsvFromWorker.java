package fr.example.formation.test;

import org.apache.spark.SparkFiles;
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
public class readCsvFromWorker {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreation")
                .master("local[*]")
                .getOrCreate();


        // plante car les fichiers ne sont pas dispo sur l'executeur
        // Création à partir de fichiers CSV
        Dataset<Row> csvDF =
                spark
                        .read()
                        .option("header", "true")
                        .csv("src/main/resources/java/dataframe/QueryOptimizations/client.csv");

        // fichier dispo sur les exe, passer un -- file
        // Création à partir de fichiers CSV
        Dataset<Row> csvDFSubmit =
                spark
                        .read()
                        .option("header", "true")
                        .csv(SparkFiles.get("client.csv"));

        // Fermer la SparkSession
        spark.stop();
    }
}
