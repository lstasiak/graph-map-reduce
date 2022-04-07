import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object Main {

  def prepareRDD(
      sc: SparkContext,
      filename: String = "web-Stanford.txt",
      ignoreRowsStart: String = "#"
  ): RDD[(Int, Int)] = {
    val rdd: RDD[(Int, Int)] =
      sc.textFile(filename).filter(!_.startsWith(ignoreRowsStart)).map { line =>
        val row = line.split("\t")
        (row(0).toInt, row(1).toInt)
      }
    rdd
  }

  def main(args: Array[String]): Unit = {
    val appName = "SparkDemo"
    val master = "local[*]"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.driver.memory", "18g")

    val sc = new SparkContext(conf)
    // turn off annotations
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val rdd: RDD[(Int, Int)] = prepareRDD(sc).repartition(8)

    val graph = new Graph(rdd)

    graph.calculateClusteringCoefficients().take(20).foreach(println)

  }
}
