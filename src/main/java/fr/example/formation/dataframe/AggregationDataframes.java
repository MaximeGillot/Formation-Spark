package fr.example.formation.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class AggregationDataframes {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreation")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> testDataDF = spark.createDataFrame(
                List.of(
                        RowFactory.create("John", 25, "USA"),
                        RowFactory.create("Alice", 25, "France"),
                        RowFactory.create("Bob", 35, "USA"),
                        RowFactory.create("Pierre", 46, "France"),
                        RowFactory.create("Mohamed", 22, "France"),
                        RowFactory.create("Thomas", 25, "France"),
                        RowFactory.create("Peter", 30, "USA")
                ),
                new StructType(new StructField[]{
                        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("pays", DataTypes.StringType, true, Metadata.empty())
                })
        );

        // Age minimun
        testDataDF
                .groupBy("pays")
                .min("age")
                .show();

        // Age maximun
        testDataDF
                .groupBy("pays")
                .max("age")
                .show();

        // aggregation
        testDataDF
                .groupBy("pays")
                .agg(
                        functions.min("age").as("age minimun"),
                        functions.max("age").as("age maximun"),
                        functions.count("name").as("nb personne"),
                        functions.avg("age").as("age moyen")
                )
                .show();


        // les 2 personnes les plus jeunes
        WindowSpec window = Window.partitionBy("pays").orderBy("age");
        testDataDF
                .withColumn("classement_age", functions.row_number().over(window))
                .show();

        //rank
        System.out.println("function rank");
        testDataDF
                .withColumn("classement_age", functions.rank().over(window))
                .show();


        // même chose que lag en SQL, l'age de la personne plus jeune juste avant
        System.out.println("function lag");
        testDataDF
                .withColumn("classement_age", functions.lag("age", 1).over(window))
                .show();

        // l'age moyen
        WindowSpec windowPays = Window.partitionBy("pays");
        testDataDF
                .withColumn("age_moyen", functions.avg(new Column("age")).over(windowPays))
                .withColumn("age_max", functions.max(new Column("age")).over(windowPays))
                .withColumn("age_min", functions.min(new Column("age")).over(windowPays))
                .withColumn("age_somme", functions.sum(new Column("age")).over(windowPays))
                .show();


        // Fermer la SparkSession
        spark.stop();
    }
}