package fr.example.formation.dataframe.exercice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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
public class Enonce {
    public static void main(String[] args) {
        // Créer une SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreation")
                .master("local[*]")
                .getOrCreate();

        // Création à partir de fichiers CSV
        Dataset<Row> csvDF = spark.read().csv("src/main/resources/java/dataframe/Exercice/Pokemon.csv");

        csvDF.show();

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Type1", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Type2", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Total", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("HP", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Attack", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Defense", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("AttackSpecial", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("DefenseSpecial", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Speed", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Generation", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Legendary", DataTypes.BooleanType, true, Metadata.empty()),

        });

        Dataset<Row> pokemonDf = spark.read().schema(schema).option("header", "true").csv("src/main/resources/java/dataframe/Exercice/Pokemon.csv");
        pokemonDf.show();


        pokemonDf
                .select("id", "Name", "Type1", "Type2", "Total", "Generation", "Legendary")
                .repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("src/main/resources/java/dataframe/Exercice/PokemonInfo");

        pokemonDf
                .select("id", "Name", "Total", "HP", "Attack", "Defense", "AttackSpecial", "DefenseSpecial", "Speed")
                .repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("src/main/resources/java/dataframe/Exercice/PokemonStats");

        // Trouvez tous les Pokémon légendaires de la première génération.

        // Combien y a-t-il de Pokémon de type "Fire" dans le jeu de données ?

        // Affichez tous les types de Pokémon uniques présents dans le jeu de données.

        // Trouvez les statistiques de base (HP, Attaque, Défense, Attaque Spéciale, Défense Spéciale et Vitesse)
        // pour tous les Pokémon de type "Water" dans le jeu de données des Pokémon.
        // Je ne veux afficher que leurs noms et leurs stats

        // Affichez les 5 Pokémon de type "Eau" avec le moins de vitesse.

        // Ajoutez une nouvelle colonne "TotalDefense" aux statistiques des Pokémon,
        // qui représente la somme de la défense (Defense) et de la défense spéciale (SP Def).

        // Affichez le plan d'execution de la précédente question

        // Combien de pokémon ont deux type ?

    }

}
