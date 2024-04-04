package fr.example.formation.rdd.exercice;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Le JDD : https://www.kaggle.com/datasets/arnavvvvv/spotify-music?resource=download
 * track_name,artist(s)_name,artist_count,released_year,released_month,released_day,in_spotify_playlists,in_spotify_charts,streams,in_apple_playlists,in_apple_charts,in_deezer_playlists,in_deezer_charts,in_shazam_charts,bpm,key,mode,danceability_%,valence_%,energy_%,acousticness_%,instrumentalness_%,liveness_%,speechiness_%
 * <p>
 * track_name
 * artist(s)_name
 * artist_count
 * released_year
 * released_month
 * released_day
 * in_spotify_playlists
 * in_spotify_charts
 * streams
 * in_apple_playlists
 * in_apple_charts
 * ...
 * <p>
 * /!\ Pour facilité son utilisation avec les RDD, le JDD à été "nettoyer".
 * Si besoin d'aide, utiliser la documentation : https://spark.apache.org/docs/latest/rdd-programming-guide.html
 *
 * Section 4: RDD
 * Session 10: Exercice
 */

public class Enonce {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("EnonceExample")
                .master("local[*]")
                .getOrCreate();

        // Chemin du fichier CSV contenant les données de Spotify
        String filePath = "src/main/resources/java/rdd/Excercice/Popular_Spotify_Songs.csv";

        JavaRDD<String> spotifyRDD = spark.sparkContext().textFile(filePath, 3).toJavaRDD();

        // Question 1 : Combien de titres différents sont répertoriés ?

        // Question 2 : Combien d'artistes différents sont répertoriés ?

        // Question 3 : Citer, si possible, 10 morceaux de musique sortis en 2014.

        // Question 4 : Quel est l'artiste le plus présent et combien de fois ?

        // Fermer la SparkSession
        spark.stop();
    }
}