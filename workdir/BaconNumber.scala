import java.io._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object BaconNumber {
  def main(args: Array[String]): Unit = {
    val targetActor = "nm0000102" // Kevin Bacon's ID
    
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    // schema for faster data loading
    val schema = new StructType()
      .add("tconst", StringType, true)
      .add("ordering", StringType, true)
      .add("nconst", StringType, true)
      .add("category", StringType, true)
      .add("job", StringType, true)
      .add("characters", StringType, true)

    // parsing the data into DataFrame
    val dataFrame = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .schema(schema)
      .csv("hdfs://namenode:9000/data/imdb/title.principals.tsv")

    // get actor pairs from the data
    val actorPairs = findActorPairs(dataFrame)

    // compute paths to Kevin Bacon
    val result = findActorShortestPaths(actorPairs, targetActor, sc)

    // write the results to a file
    val outputFile = new PrintWriter(new File("outputs/spark-bacon_number.txt"))
    result.collect().foreach(outputFile.println) // putting everything in the file
    outputFile.close()

    spark.stop()
  }

  /**
  * Extracts pairs of actors who have appeared together in the same titles.
  *
  * @param df A DataFrame containing the formatted input IMDB dataset.
  * 
  * @return An RDD containing tuples of two strings, each tuple represents a pair of actor identifiers
  *         who have appeared together in the same title.
  */
  def findActorPairs(df: DataFrame): RDD[(String, String)] = {
    // filter to only get actors and actresses, group by tconst
    df.filter($"category".isin("actor", "actress"))
      .groupBy($"tconst")
      .agg(collect_list($"nconst").alias("actors"))
      .rdd // change DataFrame to RDD

    // generate all unique 2-actor combinations from the list of actors
    .flatMap { row =>
      val listActors = row.getAs[Seq[String]]("actors")
      listActors.combinations(2) // get all pairs
        .map {
          case Seq(first, second) => (first, second)
        }
    }
    .distinct() // keep only unique pairs
  }

  /**
  * Finds shortest path distances from a target actor to all other actors using BFS.
  *
  * @param actorConnections RDD of tuples (String, String) representing the connections between actors.
  * @param targetActor The id of the target actor.
  * @param sc The SparkContext
  * @return An RDD of tuples (String, Int), where each tuple contains an actor's name and their distance from the target actor.
  */
  def findActorShortestPaths(actorConnections: RDD[(String, String)], targetActor: String, sc: org.apache.spark.SparkContext): RDD[(String, Int)] = {
    // INIT the RDD with the target actor and a distance of zero
    var distanceFromTarget = sc.parallelize(Seq((targetActor, 0))).persist()
    var activeActors: Broadcast[Set[String]] = sc.broadcast(Set(targetActor))

    // search until no active actors remain
    while (activeActors.value.nonEmpty) {
      // filter connections to find those involving at least one currently active actor
      val newActors = actorConnections
        .filter { case (src, dst) => activeActors.value.contains(src) || activeActors.value.contains(dst) }
        .flatMap { case (src, dst) =>
          // add new actor to active set by checking connectivity
          if (activeActors.value.contains(src)) Some(dst)
          else if (activeActors.value.contains(dst)) Some(src)
          else None
        }
        .distinct() // remove duplicates
        .subtract(distanceFromTarget.map(_._1)) // exclude actors already found
        .collect()
        .toSet

      // if there are new actors found, update distances and active set
      if (newActors.nonEmpty) {
        val maxDistance = distanceFromTarget.map(_._2).max()
        val newDistances = sc.parallelize(newActors.toSeq).map(actor => (actor, maxDistance + 1)).persist()
        distanceFromTarget = distanceFromTarget.union(newDistances)
        activeActors.unpersist()
        activeActors = sc.broadcast(newActors)
      } else {
        // no new actors found, clear the active actors set to exit the loop
        activeActors.unpersist()
        activeActors = sc.broadcast(Set.empty[String])
      }
    }

    distanceFromTarget
  }
}
