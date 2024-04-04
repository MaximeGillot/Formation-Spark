package fr.example.formation.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Concept fondamental : Les Datasets et Dataframes
 * Création de Dataframes et Dataframes
 */
public class OperationsDataframes {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameOperations")
                .master("local[*]")
                .getOrCreate();

        // Créer un Dataframe à partir de données simulées
        Dataset<Row> df = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "John", 30),
                        RowFactory.create(2, "Alice", 25),
                        RowFactory.create(3, "Bob", 35),
                        RowFactory.create(4, "Maxime", 25),
                        RowFactory.create(5, "Alice", 40)
                ),
                new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("age", DataTypes.IntegerType, true, Metadata.empty())
                })
        );

        // Opérations de transformation
        Dataset<Row> filteredDF = df.filter(new Column("age").gt(25));
        Dataset<Row> filteredDF2 = df.filter("age > 25 ");

        filteredDF.show();
        filteredDF2.show();


        // Opérations group by
        Dataset<Row> groupedDF = df.groupBy("age").count();
        groupedDF.show();

        // Utilisation de la fonction select pour sélectionner les colonnes id et name
        Dataset<Row> selectedDF = df.select("id", "name");
        selectedDF.show();

        // Utilisation de la fonction distinct pour obtenir les valeurs uniques de la colonne name
        Dataset<Row> distinctDF = df.select("name").distinct();
        distinctDF.show();

        // Opérations d'action
        long count = df.count();
        System.out.println("Nombre total de lignes dans le Dataframe : " + count);

        // Fermer la SparkSession
        spark.stop();
    }
}