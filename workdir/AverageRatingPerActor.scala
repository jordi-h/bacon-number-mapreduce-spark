import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AverageRatingPerActor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // principals schema for faster data loading
    val principalsSchema = new StructType()
      .add("tconst", StringType, true)
      .add("ordering", IntegerType, true)
      .add("nconst", StringType, true)
      .add("category", StringType, true)
      .add("job", StringType, true)
      .add("characters", StringType, true)

    // ratings schema for faster data loading
    val ratingsSchema = new StructType()
      .add("tconst", StringType, true)
      .add("averageRating", DoubleType, true)
      .add("numVotes", IntegerType, true)

    // load principals data
    val principalsDF = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .schema(principalsSchema)
      .csv("hdfs://namenode:9000/data/imdb/title.principals.tsv")

    // load ratings data
    val ratingsDF = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .schema(ratingsSchema)
      .csv("hdfs://namenode:9000/data/imdb/title.ratings.tsv")

    // filter for actors and actresses
    val actorsDF = principalsDF.filter($"category".isin("actor", "actress"))

    // join actors with ratings
    val joinedDF = actorsDF
      .join(ratingsDF, "tconst")
      .select("nconst", "averageRating")

    // calculate average rating per actor
    val resultDF = joinedDF
      .groupBy("nconst")
      .agg(avg("averageRating").alias("avgRating"))
      .orderBy($"avgRating".desc)

    // write the results to a file
    val outputFile = new PrintWriter(new File("outputs/spark-average_ratings_per_actor.txt"))
    resultDF.collect().foreach(outputFile.println) // putting everything in the file
    outputFile.close()

    spark.stop()
  }
}