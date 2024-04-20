package fr.example.formation.optimisation;

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
 * <a href="https://spark.apache.org/docs/latest/rdd-programming-guide.html#shared-variables">...</a>
 *
 * <a href="https://stackoverflow.com/questions/37487318/spark-sql-broadcast-hash-join">SO</a>
 */
public class SparkBroadcastJoin {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreation")
                .master("local[*]")
                .getOrCreate();

        // Création de Dataframes pour les tests unitaires
        Dataset<Row> clientDf = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "John", 20),
                        RowFactory.create(2, "Alice", 50),
                        RowFactory.create(3, "Bob", 40)
                ),
                new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("age", DataTypes.IntegerType, true, Metadata.empty())
                })
        );

        Dataset<Row> clientPays = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "Canada"),
                        RowFactory.create(2, "Maroc"),
                        RowFactory.create(3, "Canada")
                ),
                new StructType(new StructField[]{
                        new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("pays", DataTypes.StringType, true, Metadata.empty())
                })
        );


        clientDf
                .join(clientPays, clientDf.col("id").equalTo(clientPays.col("id")))
                .explain(true);

        // Fermer la SparkSession
        spark.stop();
    }
}
