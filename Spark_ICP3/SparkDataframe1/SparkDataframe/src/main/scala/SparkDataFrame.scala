import org.apache.spark.sql.SparkSession

object SparkDataFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.format("csv").option("header","true")
      .load("C:\\Users\\steph\\Downloads\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\people.csv")

    val df2 = spark.read.format("csv").option("header","true")
      .load("C:\\Users\\steph\\Downloads\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\people2.csv")


    df.show()

    print("Saving to file now")
    //df.write.parquet("C:\\Users\\steph\\Downloads\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\myFile1.parquet")

    print("Count the number of repeated records in the dataset")
    print("duplicat count: " )
    print(df.count()- df.dropDuplicates().count())

    print("Apply Union to dataset and outputting the Company alphabetically")

    //df.printSchema()
    val unionDF = df.union(df2)
    val sortDF = df.orderBy("Company")

    unionDF.show()
    sortDF.show()

    print("Group by Zip codes")

    val groupDF = df.groupBy("Zip Code").count().show()

    print("Join 1")

    val joinedDF = df.join(df2, "Company")
    joinedDF.show()

    print("Aggregate 1")

    val aggDF = df.groupBy("Complaint ID").sum().show()

    print("Fetching 13th row")

    val rowThirteen = df.rdd.take(13).last
    println(rowThirteen)

  }
}
