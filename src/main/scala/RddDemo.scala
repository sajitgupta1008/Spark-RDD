import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RddDemo extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RddDemo")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val pagecounts = sparkSession.sparkContext.textFile("/home/knoldus/IdeaProjects/SparkAssignment1/src/main/resources/pagecounts-20151201-220000")

  def writeToFile(content: String) {
    val writer = new PrintWriter(new File("/home/knoldus/IdeaProjects/SparkAssignment1/src/main/resources/output.txt"))
    writer.write(content)
    writer.close
  }

  //RDD containing first 10 records
  val first10Records = pagecounts.zipWithIndex().filter {
    case (value, index) => index >= 0 && index <= 9
  }.keys

  writeToFile(first10Records.collect.mkString("\n"))

  println("Total records count : " + pagecounts.count())          //OUTPUT : 7598006

  val pageArray = pagecounts.map(_.split(" "))

  //RDD containing english pages only
  val engPages = pageArray.filter(line => line(0).equals("en"))

  println("\nEnglish pages count : " + engPages.count)            //OUTPUT : 2278417

  //RDD containing pages requested more than 200,000 times
  val pagesMoreThan200000 = pageArray.map(arr => (arr(1), arr(2).toLong)).reduceByKey(_ + _).filter { case (_, num) => num > 200000 }

  pagesMoreThan200000.collect().foreach(println)
}
