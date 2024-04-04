package fr.exemple.formation.installation

import org.apache.spark.sql.SparkSession

object Installation {

  def main(args: Array[String]): Unit = {
    // Créer une SparkSession
    val spark = SparkSession
      .builder
      .appName("SparkSessionExampleFormation")
      .master("local[*]")
      .getOrCreate

    // Afficher la version de Spark
    val sparkVersion = spark.version

    println(s"Version de Spark: $sparkVersion")
    // Fermer la SparkSession
    spark.stop()
  }

}
