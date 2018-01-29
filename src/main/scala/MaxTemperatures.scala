import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Find the maximum temperature by weather station for a year */
object MaxTemperatures {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val day = fields(1)
    val entryType = fields(2)
    //    val temperature = fields(3).toFloat * 0.1f // Celsius
    //    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f // Fahrenheit
    val precipitation = fields(3).toInt
    (day, entryType, precipitation)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxTemperatures")

    val lines = sc.textFile("extras/1800.csv")
    val parsedLines = lines.map(parseLine)
    //    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    //    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    //    val maxTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y))
    //    val results = maxTempsByStation.collect()
    //
    //    for (result <- results.sorted) {
    //       val station = result._1
    //       val temp = result._2
    //       val formattedTemp = f"$temp%.1f C"
    //       println(s"$station max temperature: $formattedTemp")
    //    }

    // remove not PRCP lines
    val precipitationsOnly = parsedLines.filter(l => l._2 == "PRCP")

    // remove PRCP column
    val precipitations = precipitationsOnly.map(p => (p._1, p._3))

    // get max PRCP
    val maxPrecipitations = precipitations.reduceByKey((p1, p2) => max(p1, p2))

    // run computations
    val results = maxPrecipitations.collect()

    for (result <- results.sorted) {
      println(s"Day: ${result._1}, precipitation: ${result._2}")
    }


  }
}
