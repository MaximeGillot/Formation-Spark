package fr.example.formation.sparkSql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Présentation de l'interopérabilité entre Spark SQL et les Datasets/DataFrames.
 * Utilisation de SQL pour interroger et manipuler des Datasets/DataFrames.
 * Avantages de l'utilisation de Spark SQL pour les traitements structurés.
 * https://stackoverflow.com/questions/66356293/dataframe-api-vs-spark-sql/66360355#66360355
 */
public class IntegrationDataframesSparkSql {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("IntegrationDataframesSparkSql")
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

        // Exécution d'une requête SQL sur le DataFrame
        testDataDF.createOrReplaceTempView("donnees");
        Dataset<Row> resultSQL = spark.sql("SELECT id FROM donnees WHERE id >= 2 LIMIT 1");

        Dataset<Row> resultDf =
                testDataDF
                        .filter(new Column("id").geq(2))
                        .select("id")
                        .limit(1);

        resultSQL.show();
        resultSQL.explain(true);

        /*
            == Analyzed Logical Plan ==
            id: int
            GlobalLimit 1
            +- LocalLimit 1
               +- Project [id#0]
                  +- Filter (id#0 >= 2)
                     +- SubqueryAlias donnees
                        +- View (`donnees`, [id#0,name#1])
                           +- LocalRelation [id#0, name#1]
        */

        resultDf.show();
        resultDf.explain(true);
        /*
        == Analyzed Logical Plan ==
        id: int
        GlobalLimit 1
        +- LocalLimit 1
           +- Project [id#0]
              +- Filter (id#0 >= 2)
                 +- LocalRelation [id#0, name#1]
        */

        // Fermer la SparkSession
        spark.stop();
    }
}
