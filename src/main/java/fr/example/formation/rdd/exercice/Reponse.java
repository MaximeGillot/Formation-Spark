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
 */

public class Reponse {
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

        // Exercices pour manipuler les RDD

        // Question 1 : Combien de titres différents sont répertoriés dans les données ?
        long countTracks = spotifyRDD.map(line -> line.split(",")[0]).distinct().count();
        System.out.println("Nombre de titres différents : " + countTracks);

        // Question 2 : Combien d'artistes différents sont répertoriés dans les données ?
        long countArtists = spotifyRDD.map(line -> line.split(",")[1]).distinct().count();
        System.out.println("Nombre d'artistes différents : " + countArtists);


        // Question 3 : Citer, si possible, 10 morceaux de musique sortis en 2014.

        // Filtrer les morceaux de musique sortis en 2014
        JavaRDD<String> spotify2014SongRDD = spotifyRDD.filter(line -> {
            String[] parts = line.split(",");
            int releasedYear = Integer.parseInt(parts[3]); // Indice de la colonne released_year
            return releasedYear == 2014;
        });

        // Afficher les résultats
        System.out.println("Morceaux de musique sortis en 2014 :");
        spotify2014SongRDD.map(line -> line.split(",")[0]).take(10).forEach(System.out::println);

        // Question 4 : Quel est l'artiste le plus présent et combien de fois ?
        JavaPairRDD<String, Integer> artistCountsRDD = spotifyRDD.mapToPair(line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(parts[1], 1);
        });

        // Additionner le nombre d'occurrences de chaque artiste
        JavaPairRDD<String, Integer> totalArtistCountsRDD = artistCountsRDD.reduceByKey(Integer::sum);

        // Trouver l'artiste avec le nombre maximum d'occurrences
        Tuple2<String, Integer> mostFrequentArtist = totalArtistCountsRDD.reduce((t1, t2) -> t1._2() > t2._2() ? t1 : t2);

        // Afficher le résultat
        System.out.println("L'artiste le plus présent dans le jeu de données est : " + mostFrequentArtist._1() +
                " avec un total de " + mostFrequentArtist._2() + " occurrences.");


        // Fermer la SparkSession
        spark.stop();
    }
}