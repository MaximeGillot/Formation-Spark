package fr.example.formation.dataframe.dataset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StrongTypingAndDatasetSecurity {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("StrongTypingAndDatasetSecurity")
                .master("local[*]") // Mode local pour les tests
                .getOrCreate();


        StructType schema = new StructType(new StructField[]{
                new StructField("clientId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),

        });

        Dataset<Client> clientsDS =
                spark
                        .read()
                        .option("header", "true")
                        .schema(schema)
                        .csv("src/main/resources/java/dataset/client.csv")
                        .as(Encoders.bean(Client.class));

        clientsDS.show();
        clientsDS.printSchema();

        //   Filtrer les clients adultes (typage fort)
        Dataset<Client> adultsDS = clientsDS.filter((FilterFunction<Client>) client -> client.getAge() >= 18);
        adultsDS.show();


        Dataset<Client> adultsDS2 = clientsDS.filter(Client::isLegalAge);
        adultsDS2.show();

        // Afficher les clients adultes
        adultsDS.show();

        // Fermer la SparkSession
        spark.stop();
    }
}