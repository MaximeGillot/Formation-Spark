package fr.example.formation.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.jdk.CollectionConverters;

import java.util.List;

/**
 * Compréhension de l'importance du partitionnement des RDD :
 * Le partitionnement des RDD est crucial pour la répartition efficace des données sur les nœuds du cluster.
 * Une bonne répartition des données assure une utilisation équilibrée des ressources du cluster et une exécution plus rapide des opérations parallèles.
 */
public class RDDPartitioningExample {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("RDDPartitioningExample")
                .master("local[*]")
                .getOrCreate();

        List<Integer> data = java.util.Arrays.asList(1, 2, 3, 4, 5, 5);
        String destinationDefault = "src/main/resources/java/rdd/RDDPartitioningExample/default";
        String destinationRepartition = "src/main/resources/java/rdd/RDDPartitioningExample/repartition";

        // Créer un RDD à partir d'une liste
        JavaRDD<Integer> dataRDD = spark.sparkContext().parallelize(
                CollectionConverters.IteratorHasAsScala(data.iterator()).asScala().toSeq(), 1, scala.reflect.ClassTag$.MODULE$.apply(Integer.class)
        ).toJavaRDD();

        // Afficher le nombre de partitions par défaut
        System.out.println("Nombre de partitions par défaut : " + dataRDD.getNumPartitions());

        // Sauvegarder le RDD sous forme de fichier texte
        dataRDD.saveAsTextFile(destinationDefault);

        // Repartitionner le RDD en 4 partitions
        JavaRDD<Integer> repartitionedRDD = dataRDD.repartition(4);

        // Afficher le nombre de partitions après le repartitionnement
        System.out.println("Nombre de partitions après le repartitionnement : " + repartitionedRDD.getNumPartitions());

        // Sauvegarder le RDD sous forme de fichier texte
        repartitionedRDD.saveAsTextFile(destinationRepartition);

        // Stratégies de partitionnement
        // 1. Partitionnement par défaut : basé sur la source de données et le nombre de nœuds dans le cluster
        // 2. Repartitionnement :  Cette opération redistribue les données sur un nombre spécifié de partitions, ce qui peut être utile pour rééquilibrer la charge ou augmenter la parallélisme.
        // 3. Coalesce : Cette opération fusionne les partitions adjacentes pour réduire le nombre total de partitions, ce qui peut être utile pour optimiser les opérations de réduction ou de jointure.

        // Fermer la SparkSession
        spark.stop();
    }
}
