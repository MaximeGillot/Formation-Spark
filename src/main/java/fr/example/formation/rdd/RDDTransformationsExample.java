package fr.example.formation.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.jdk.CollectionConverters;

import java.util.Arrays;
import java.util.List;

public class RDDTransformationsExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("RDDTransformationsExample")
                .master("local[*]")
                .getOrCreate();

        List<Integer> data = java.util.Arrays.asList(1, 2, 3, 4, 5, 5);

        // Créer un RDD à partir d'une liste
        JavaRDD<Integer> rdd = spark.sparkContext().parallelize(
                CollectionConverters.IteratorHasAsScala(data.iterator()).asScala().toSeq(), 1, scala.reflect.ClassTag$.MODULE$.apply(Integer.class)
        ).toJavaRDD();

        // Transformation map: multiplier chaque élément par 2
        JavaRDD<Integer> mappedRDD = rdd.map(x -> x * 2);
        // Afficher les résultats
        System.out.println("Transformation map:");
        mappedRDD.collect().forEach(System.out::println);

        // Transformation filter: filtrer les éléments supérieurs à 3
        JavaRDD<Integer> filteredRDD = rdd.filter(x -> x > 3);
        System.out.println("Transformation filter:");
        filteredRDD.collect().forEach(System.out::println);

        // Transformation flatMap: séparer chaque élément en mots
        JavaRDD<String> wordsRDD = rdd.flatMap(x -> Arrays.asList(String.valueOf(x), "a").iterator());
        System.out.println("Transformation flatMap:");
        wordsRDD.collect().forEach(System.out::println);

        // Transformation Distinct: Dédoublonner un RDD
        JavaRDD<Integer> distinctRdd = rdd.distinct();
        System.out.println("Transformation Distinct:");
        distinctRdd.collect().forEach(System.out::println);

        // Fermer la SparkSession
        spark.stop();
    }
}
