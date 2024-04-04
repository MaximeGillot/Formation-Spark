package fr.example.formation.dataframe.exercice;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * https://www.kaggle.com/datasets/abcsds/pokemon/data
 * On split le JDD en 2 pour faire des jointures.
 * le JDD : les pokémon
 * id: ID for each pokemon
 * Name: Name of each pokemon
 * Type 1: Each pokemon has a type, this determines weakness/resistance to attacks
 * Type 2: Some pokemon are dual type and have 2
 * Total: sum of all stats that come after this, a general guide to how strong a pokemon is
 * HP: hit points, or health, defines how much damage a pokemon can withstand before fainting
 * Attack: the base modifier for normal attacks (eg. Scratch, Punch)
 * Defense: the base damage resistance against normal attacks
 * SP Atk: special attack, the base modifier for special attacks (e.g. fire blast, bubble beam)
 * SP Def: the base damage resistance against special attacks
 * Speed: determines which pokemon attacks first each round
 * Generation: wich geneartion is this pokemon
 * Legendary: define if a pokemon is legendary true/false
 */
public class Reponse {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreation")
                .master("local[*]")
                .getOrCreate();

        // Création à partir de fichiers CSV

        StructType schemaInfo = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Type1", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Type2", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Total", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Generation", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Legendary", DataTypes.BooleanType, true, Metadata.empty()),

        });

        Dataset<Row> pokemonDF = spark.read().schema(schemaInfo).option("header", "true").csv("src/main/resources/java/Exercice/PokemonInfo/*.csv");

        /**
         * Trouvez tous les Pokémon légendaires de la première génération.
         */

        // Filtrer les Pokémon légendaires de la première génération
        Dataset<Row> legendaryFirstGenPokemon = pokemonDF.filter("Generation == 1 AND Legendary == true");
        // Afficher les résultats
        legendaryFirstGenPokemon.show(false);

        /**
         * Combien y a-t-il de Pokémon de type "Fire" dans le jeu de données ?
         */

        // Filtrer les Pokémon de type "Feu"
        Dataset<Row> fireTypePokemon = pokemonDF.filter("Type1 == 'Fire' OR Type2 == 'Fire'");

        // Compter le nombre de Pokémon de type "Feu"
        long fireTypePokemonCount = fireTypePokemon.count();

        // Afficher le nombre de Pokémon de type "Feu"
        System.out.println("Nombre de Pokémon de type \"Feu\" : " + fireTypePokemonCount);

        /**
         * Affichez tous les types de Pokémon uniques présents dans le jeu de données.
         */

        // Extraire tous les types de Pokémon uniques
        Dataset<Row> uniquePokemonTypes = pokemonDF.select("Type1").union(pokemonDF.select("Type2")).distinct();

        // Afficher tous les types de Pokémon uniques
        uniquePokemonTypes.show(false);

        /**
         * Trouvez les statistiques de base (HP, Attaque, Défense, Attaque Spéciale, Défense Spéciale et Vitesse)
         * pour tous les Pokémon de type "Water" dans le jeu de données des Pokémon.
         */

        StructType schemaStats = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Total", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("HP", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Attack", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Defense", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("AttackSpecial", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("DefenseSpecial", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Speed", DataTypes.IntegerType, true, Metadata.empty())
        });

        Dataset<Row> statsDF = spark.read().schema(schemaStats).option("header", "true").csv("src/main/resources/java/Exercice/PokemonStats/*.csv");


        // Filtrer les Pokémon de type "Water"
        Dataset<Row> waterTypePokemon = pokemonDF
                .filter("Type1 == 'Water' OR Type2 == 'Water'")
                .withColumnRenamed("Name", "NameWater");

        // Effectuer la jointure avec les statistiques des Pokémon en utilisant l'ID et le nom
        Dataset<Row> waterTypePokemonStats = waterTypePokemon.join(statsDF,
                        waterTypePokemon.col("id").equalTo(statsDF.col("id")).and(waterTypePokemon.col("NameWater").equalTo(statsDF.col("Name"))))
                .select("Name", "HP", "Attack", "Defense", "AttackSpecial", "DefenseSpecial", "Speed");

        // Afficher les statistiques des Pokémon de type "Water"
        waterTypePokemonStats.show();

        /**
         * Affichez les 5 Pokémon de type "Eau" avec le moins de vitesse.
         */

        Dataset<Row> sortedWaterTypePokemon =
                waterTypePokemonStats
                        .orderBy(new Column("Speed").asc())
                        .limit(5);
        sortedWaterTypePokemon.show();

        // Ajoutez une nouvelle colonne "TotalDefense" aux statistiques des Pokémon,
        // qui représente la somme de la défense (Defense) et de la défense spéciale (SP Def).
        Dataset<Row> pokemonWithTotalDefense = statsDF
                .withColumn("TotalDefense",
                        new Column("Defense").plus(new Column("DefenseSpecial"))
                );
        pokemonWithTotalDefense.show();

        // Affichez le plan d'execution de la précédente question
        pokemonWithTotalDefense.explain(true);


        /**
         * Combien de pokémon ont deux type ?
         */
        // Filtrer les Pokémon avec un seul type
        Dataset<Row> doubleTypePokemon = pokemonDF.na().drop(new String[]{"Type2"});

        // Compter le nombre de Pokémon avec 2 types
        long doubleTypePokemonCount = doubleTypePokemon.count();

        // Afficher le nombre de Pokémon avec 2 types
        System.out.println("Nombre de Pokémon avec double type : " + doubleTypePokemonCount);



    }

}
