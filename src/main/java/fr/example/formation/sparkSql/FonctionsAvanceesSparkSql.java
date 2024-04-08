package fr.example.formation.sparkSql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.UUID;

import static org.apache.spark.sql.functions.udf;

/**
 * Présentation des fonctionnalités avancées de Spark SQL.
 * Support des opérations relationnelles : jointures, groupages, agrégations, etc.
 * Utilisation de fonctions SQL et de fonctions intégrées pour la manipulation des données.
 */
public class FonctionsAvanceesSparkSql {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("FonctionsAvanceesSparkSql")
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

        // Exécution d'une requête SQL sur le DataFrame
        clientDf.createOrReplaceTempView("client");
        clientPays.createOrReplaceTempView("paysClient");

        Dataset<Row> result = spark.sql("SELECT paysClient.pays, SUM(client.age) AS ageTotal, AVG(client.age) AS ageMoyen " +
                "FROM client JOIN paysClient ON client.id = paysClient.id group by paysClient.pays");

        result.show();

        UserDefinedFunction random = udf(
                () -> UUID.randomUUID().toString(), DataTypes.StringType
        );

        random.asNondeterministic();
        spark.udf().register("randomUUID", random);

        result = spark.sql("SELECT randomUUID() as uuid, paysClient.pays, SUM(client.age) AS ageTotal, AVG(client.age) AS ageMoyen " +
                "FROM client JOIN paysClient ON client.id = paysClient.id group by paysClient.pays");

        result.show(false);

        // Udf avec parametre

        UserDefinedFunction plus5Ans = udf(
                (UDF1<Integer, Integer>) age -> age + 5, DataTypes.IntegerType
        );

        spark.udf().register("plus5Ans", plus5Ans);
        spark.sql("SELECT id, name, age , plus5Ans(age) as agePlus5Ans from client").show();
        spark.sql("SELECT id, name, age from client where plus5Ans(age) > 50").show();

        // UDF avec l'api dataframe
        clientDf
                .withColumn(
                        "age plus 5 ans",
                        functions.callUDF("plus5Ans", new Column("age"))
                ).show();

        // Fermer la SparkSession
        spark.stop();
    }
}
