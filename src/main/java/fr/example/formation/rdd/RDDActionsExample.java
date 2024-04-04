package fr.example.formation.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.jdk.CollectionConverters;

import java.util.List;


public class RDDActionsExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("RDDActionsExample")
                .master("local[*]")
                .getOrCreate();

        List<Integer> data = java.util.Arrays.asList(1, 2, 3, 4, 5, 5);

        // Créer un RDD à partir d'une liste
        JavaRDD<Integer> numbersRDD = spark.sparkContext().parallelize(
                CollectionConverters.IteratorHasAsScala(data.iterator()).asScala().toSeq(), 1, scala.reflect.ClassTag$.MODULE$.apply(Integer.class)
        ).toJavaRDD();

        // Compter le nombre d'éléments dans le RDD (Action)
        long count = numbersRDD.count();
        System.out.println("Nombre d'éléments dans le RDD : " + count);

        // Collecter les éléments du RDD en tant que tableau local (Action)
        List<Integer> collected = numbersRDD.collect();
        System.out.println("Contenu du RDD : " + collected.toString());

        // Prendre les premiers éléments du RDD (Action)
        List<Integer> taken = numbersRDD.take(3);
        System.out.println("Les 3 premiers éléments du RDD : " + taken.toString());

        String destination = "src/main/resources/java/rdd/RDDActionsExample/saveAsTextFile";
        // Sauvegarder le RDD sous forme de fichier texte
        numbersRDD.saveAsTextFile(destination);

        // Fermer la SparkSession
        spark.stop();
    }
}

