package fr.example.formation.installation;

import org.apache.spark.sql.SparkSession;

/**
 * Dans les variables d'env:
 * Ajouter HADOOP_HOME qui pointe vers: E:\Big_Data\Cours Spark\winutils\hadoop-3.3.5
 * Ajouter dans le PATH : %HADOOP_HOME%\bin
 * <p>
 * Si on utilise Java 17, il faut passer en argument de la JVM : --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
 * script ici : <a href="https://github.com/apache/spark/blob/v3.3.0/launcher/src/main/java/org/apache/spark/launcher/JavaModuleOptions.java">...</a>
 * SO: <a href="https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct">...</a>
 */
public class Installation {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSessionExampleFormation")
                .master("local[*]")
                .getOrCreate();

        // Afficher la version de Spark
        String sparkVersion = spark.version();
        System.out.println("Version de Spark: " + sparkVersion);

        // Fermer la SparkSession
        spark.stop();
    }
}