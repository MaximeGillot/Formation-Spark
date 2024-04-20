package fr.example.formation.optimisation;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 *
 * <a href="https://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables">broadcast-variables</a>
 */
public class SparkBroadcastVariable {
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

        Broadcast<String> broadcastVar = spark.sparkContext().broadcast("string broadcaster", scala.reflect.ClassManifestFactory.fromClass(String.class));

        clientDf = clientDf.withColumn("broadcastVariable", functions.lit(broadcastVar.getValue()));
        clientDf.show();

        broadcastVar.unpersist();
        broadcastVar.destroy();

        clientDf.explain(true);

        // Fermer la SparkSession
        spark.stop();
    }
}
