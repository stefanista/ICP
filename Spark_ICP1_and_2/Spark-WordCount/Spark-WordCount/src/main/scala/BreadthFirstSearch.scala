import org.apache.spark.sql.SparkSession

object BreadthFirstSearch {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Winutils");

    val sc = SparkSession
      .builder
      .appName("SparkWordCount")
      .master("local[*]")
      .getOrCreate().sparkContext

    val input= sc.textFile("input2")







  }

}
