package fr.example.formation.sparkUI;

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
 * programme simple pour SPark UI
 */
public class sparkUISimple {
    public static void main(String[] args) throws InterruptedException {
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
        Dataset<Row> filteredDF = df.filter(df.col("age").gt(25));

        df.cache();

        filteredDF.show();

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

        Thread.sleep(100000000L);
        // Fermer la SparkSession
        spark.stop();
    }
}