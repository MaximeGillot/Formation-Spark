package fr.example.formation.optimisation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Function1;

import java.util.Arrays;

/**
 * <a href="https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators">broadcast-variables</a>
 */
public class SparkAccumulators {
    public static void main(String[] args) throws InterruptedException {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkAccumulators")
                .master("local[*]")
                .getOrCreate();

        Encoder<Long> longEncoders = Encoders.LONG();

        Dataset<Long> longDS = spark.createDataset(Arrays.asList(20L, 30L, 40L), longEncoders);

        DoubleAccumulator nbLigne = spark.sparkContext().doubleAccumulator("nbLigne");

        longDS.foreach(x -> {
            nbLigne.add(1);
        });

        System.out.println("nb ligne: " + nbLigne.value());

        LongAccumulator nbAge = spark.sparkContext().longAccumulator("total age");

        longDS.foreach(x -> {
            nbAge.add(x);
        });

        System.out.println("age total : " + nbAge.value());
        System.out.println("nb ligne : " + nbAge.count());
        System.out.println("age moyen : " + nbAge.avg());
        System.out.println("somme age : " + nbAge.sum());

        LongAccumulator testLazyEvaluation = spark.sparkContext().longAccumulator();

        longDS.map((Function1<Long, Long>) x -> {
            testLazyEvaluation.add(1);
            return x;
        }, longEncoders);

        System.out.println("value à 0 ?: " + testLazyEvaluation.value());


        Thread.sleep(1000000l);
        // Fermer la SparkSession
        spark.stop();
    }
}
