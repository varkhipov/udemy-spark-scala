import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCounterExtended extends App {
  // Set the log level to only print errors
  //  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using the local machine
  val sc = new SparkContext("local[*]", "RatingsCounterExtended")

  // Load lines into an RDDs
  val dataRDD = sc.textFile("../ml-100k/u.data")
  val moviesRDD = sc.textFile("../ml-100k/u.item")

  //TODO: to be continued...

}
