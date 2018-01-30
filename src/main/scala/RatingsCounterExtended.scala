import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCounterExtended extends App {
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using the local machine
  val sc = new SparkContext("local[*]", "RatingsCounterExtended")

  // Load RDDs
  val dataRDD = sc.textFile("../ml-100k/u.data").cache()   // user id  | item id     | rating       | timestamp
  val moviesRDD = sc.textFile("../ml-100k/u.item").cache() // movie id | movie title | release date | ...

  // Map to set of tuples --> (id, (rating, 1))
  val dataIdsAndRatings = dataRDD.map(line => {
    val splitted = line.split("\\W+")
    val id = splitted(1).toInt
    val rating = splitted(2).toInt
    (id, (rating, 1))
  })

  // Map to set of tuples --> (id, title)
  val moviesIdsAndTitles = moviesRDD.map(line => {
    val splitted = line.split("\\|")
    val id = splitted(0).toInt
    val title = splitted(1)
    (id, title)
  })


  // Count total ratings and total votes for each movie --> (id, (total_rating, total_votes_count))
  val dataIdsAndTotalRatings = dataIdsAndRatings.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

  // Count avg rating for each movie --> (id, avg_rating)
  val dataIdsAndAvgRatings = dataIdsAndTotalRatings.mapValues(x => x._1 / x._2)


  // Left Join both RDDs --> (id, (avg_rating, title: Option[String]))
  val idsAndAvgRatingsAndTitles = dataIdsAndAvgRatings.leftOuterJoin(moviesIdsAndTitles)

  // Remove id column, extract optional value of title --> (avg_rating, title)
  val titlesAndAvgRatings = idsAndAvgRatingsAndTitles.map(x => (x._2._1, x._2._2.getOrElse("N/A").trim))

  // Sort by rating(desc) and then by title(asc)
  val titlesAndAvgRatingsSorted = titlesAndAvgRatings.sortBy(r => (- r._1, r._2)).collect()

  printf("%s  |  %s\n----------------------------------------------------------------------------\n", "rating", "title")
  titlesAndAvgRatingsSorted.foreach(l => { printf("%4s    |  %-5s\n", l._1, l._2) })
  println(s"\n\nTotal: ${titlesAndAvgRatingsSorted.length}")




  //TODO: the same with Spark SQL



  // Finish job
  sc.stop()
}
