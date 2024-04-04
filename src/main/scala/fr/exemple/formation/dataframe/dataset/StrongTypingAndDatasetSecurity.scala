package fr.exemple.formation.dataframe.dataset

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

object StrongTypingAndDatasetSecurity {

  def main(args: Array[String]): Unit = {
    // Créer une SparkSession
    val spark = SparkSession
      .builder
      .appName("StrongTypingAndDatasetSecurity")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val schema = new StructType(Array[StructField](
      StructField("clientId", DataTypes.StringType),
      StructField("name", DataTypes.StringType),
      StructField("age", DataTypes.IntegerType)
    )
    )

    val clientsDS: Dataset[Client] = spark
      .read
      .option("header", "true")
      .schema(schema)
      .csv("src/main/resources/java/dataset/client.csv")
      .as[Client]

    // Afficher les clients
    clientsDS.show()

    val adultsDS = clientsDS.filter(x => x.isLegalAge)

    // Afficher les clients adultes
    adultsDS.show()

    // Fermer la SparkSession
    spark.stop()
  }

}
