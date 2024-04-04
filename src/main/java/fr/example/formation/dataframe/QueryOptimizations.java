package fr.example.formation.dataframe;

import org.apache.spark.sql.*;

public class QueryOptimizations {

    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("QueryOptimizations")
                .master("local[*]")
                .getOrCreate();

        // Charger un fichier CSV de données client dans un DataFrame
        Dataset<Row> clientDF = spark.read()
                .option("header", true)
                .csv("src/main/resources/java/dataframe/QueryOptimizations/client.csv");

        // Charger un fichier CSV de données Pays Client dans un DataFrame
        Dataset<Row> paysClientDF = spark.read()
                .option("header", true)
                .csv("src/main/resources/java/dataframe/QueryOptimizations/clientPays.csv");

        // Afficher le schéma du DataFrame
        System.out.println("Schéma du DataFrame :");
        clientDF.printSchema();

        // Projection sélective
        Dataset<Row> clientProjectedDF = clientDF.select("clientId", "age");

        // Filtrage pushdown
        Dataset<Row> clientFilteredDF = clientProjectedDF.filter("age >= 18");

        // Prédicats de jointure
        Dataset<Row> clientsPaysDF = clientFilteredDF.join(paysClientDF, "clientId");

        clientsPaysDF.cache();
        clientsPaysDF.show();

        Dataset<Row> clientsPaysCsvDF = clientsPaysDF.withColumn("type", functions.lit("csv"));
        Dataset<Row> clientsPaysParquetDF = clientsPaysDF.withColumn("type", functions.lit("parquet"));

        clientsPaysCsvDF.write().mode(SaveMode.Overwrite).csv("src/main/resources/java/dataframe/QueryOptimizations/write/resultCsv");
        clientsPaysParquetDF.write().mode(SaveMode.Overwrite).parquet("src/main/resources/java/dataframe/QueryOptimizations/write/resultParquet");

        clientsPaysParquetDF.explain(true);

        // Fermer la SparkSession
        spark.stop();
    }
}
