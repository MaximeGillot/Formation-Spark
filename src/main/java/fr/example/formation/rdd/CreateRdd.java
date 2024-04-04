package fr.example.formation.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.jdk.CollectionConverters;

import java.util.List;

/*
 * JavaRDD vs RDD : There's no significant performance penalty - JavaRDD is a simple wrapper around RDD just to make calls from Java code more convenient. It holds the original RDD as its member, and calls that member's method on any method invocation
 */
public class CreateRdd {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("RDDCreationExample")
                .master("local[*]")
                .getOrCreate();

        /*
         * Données local
         */
        List<Integer> data = java.util.Arrays.asList(1, 2, 3, 4, 5);

        // Création de RDD à partir de données locales avec la méthode parallelize
        JavaRDD<Integer> localDataRDD = spark.sparkContext().parallelize(
                CollectionConverters.IteratorHasAsScala(data.iterator()).asScala().toSeq(), 1, scala.reflect.ClassTag$.MODULE$.apply(Integer.class)
        ).toJavaRDD();

        // Afficher les données des RDD
        System.out.println("Données du RDD créé à partir de données locales:");
        localDataRDD.collect().forEach(System.out::println);

        /*
         * Local
         */
        String localTextFilePath = "src/main/resources/java/rdd/CreateRdd/prenom.txt";
        // Création de RDD à partir d'un fichier texte sur HDFS avec la méthode textFile
        JavaRDD<String> localTextFileRDD = spark.sparkContext().textFile(localTextFilePath, 1).toJavaRDD();
        System.out.println("\nDonnées du RDD créé à partir du fichier sur le FS:");
        localTextFileRDD.take(5).forEach(System.out::println); // Afficher les 5 premières lignes

        /*
         * HDFS
         */
        // Chemin du fichier sur HDFS
        String hdfsFilePath = "hdfs://path/to/your/file.txt";
        // Création de RDD à partir d'un fichier texte sur HDFS avec la méthode textFile
        JavaRDD<String> hdfsDataRDD = spark.sparkContext().textFile(hdfsFilePath, 1).toJavaRDD();
        System.out.println("\nDonnées du RDD créé à partir du fichier sur HDFS:");
        hdfsDataRDD.take(5).forEach(System.out::println); // Afficher les 5 premières lignes

        /*
         * S3
         */
        // Chemin du fichier sur S3
        String s3FilePath = "s3://your-bucket/path/to/your/file.txt";
        // Création de RDD à partir d'un fichier texte sur S3 avec la méthode textFile
        JavaRDD<String> s3DataRDD = spark.sparkContext().textFile(s3FilePath, 1).toJavaRDD();
        System.out.println("\nDonnées du RDD créé à partir du fichier sur S3:");
        s3DataRDD.take(5).forEach(System.out::println); // Afficher les 5 premières lignes


        // Fermer la SparkSession
        spark.stop();
    }
}