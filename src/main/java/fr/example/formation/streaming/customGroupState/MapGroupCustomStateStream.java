package fr.example.formation.streaming.customGroupState;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class MapGroupCustomStateStream {

    public static MapGroupsWithStateFunction<String, Row, CustomGroupStateStore, Row> mapGroupsWithStateFunction = (key, values, state) -> {
        int sum;
        CustomGroupStateStore stateStore = state.exists() ? state.get() : new CustomGroupStateStore();
        // Si l'état n'existe pas, on initialise le compteur de production
        sum = stateStore.getNbProduction();
        for (Iterator<Row> it = values; it.hasNext(); ) {
            Row currentRow = it.next();
            sum += currentRow.getInt(currentRow.fieldIndex("production"));
        }
        stateStore.setNbProduction(sum);
        state.update(stateStore);
        return RowFactory.create(key, sum);
    };


    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkStreamMapGroupsWithState")
                .master("local[*]")
                .getOrCreate();

        // Définir le schéma du CSV
        StructType csvSchema = new StructType()
                .add("name", "string")
                .add("production", "integer");


        // Lire le flux CSV
        Dataset<Row> csvDF = spark
                .readStream()
                .option("header", true)
                .schema(csvSchema)
                .csv("src/main/resources/java/streaming/mapGroupsWithState/in");


        csvDF = csvDF
                .groupByKey((MapFunction<Row, String>) row -> row.getAs("name"), Encoders.STRING())
                .mapGroupsWithState(
                        mapGroupsWithStateFunction,
                        Encoders.bean(CustomGroupStateStore.class),
                        Encoders.row(csvSchema),
                        GroupStateTimeout.NoTimeout());


        // Écrire le résultat dans la console
        StreamingQuery query = csvDF.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();
    }

}
