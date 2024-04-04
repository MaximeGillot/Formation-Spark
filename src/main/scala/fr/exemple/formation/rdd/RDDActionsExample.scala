package fr.exemple.formation.rdd

import org.apache.spark.sql.SparkSession

object RDDActionsExample {
  def main(args: Array[String]): Unit = {
    // Créer une SparkSession
    val spark: SparkSession = SparkSession
      .builder
      .appName("RDDActionsExample")
      .master("local[*]")
      .getOrCreate

    val data: Seq[Integer] = Seq(1, 2, 3, 4, 5, 5)

    // Créer un RDD à partir d'une liste
    val numbersRDD = spark.sparkContext.parallelize(data)

    // Compter le nombre d'éléments dans le RDD (Action)
    val count: Long = numbersRDD.count
    System.out.println("Nombre d'éléments dans le RDD : " + count)

    // Collecter les éléments du RDD en tant que tableau local (Action)
    val collected = numbersRDD.collect.toArray
    System.out.println("Contenu du RDD : " + collected.mkString("Array(", ", ", ")"))


    // Prendre les premiers éléments du RDD (Action)
    val taken = numbersRDD.take(3).toArray
    System.out.println("Les 3 premiers éléments du RDD : " + taken.mkString("Array(", ", ", ")"))


    // Sauvegarder le RDD sous forme de fichier texte
    val destination: String = "src/main/resources/scala/rdd/RDDActionsExample/saveAsTextFile"
    numbersRDD.saveAsTextFile(destination)

    // Fermer la SparkSession
    spark.stop()
  }
}
