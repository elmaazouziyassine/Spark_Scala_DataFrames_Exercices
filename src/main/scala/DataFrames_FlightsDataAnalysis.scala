import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object DataFrames_FlightsDataAnalysis {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    // Create the SparkSession Object : spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Flights Data Analysis")
      .getOrCreate()

    // To be able to use the $-notation & for implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Create a dataframe from the external csv data file
    val dataDF = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("src/datasets/flightsData2015.csv")

    // Print 10 first elements
    dataDF.show(10)

    // Print the schema
    dataDF.printSchema()

    // Explore Data
    val nmbrOfOriginCoutntries = dataDF.select("ORIGIN_COUNTRY_NAME").distinct().count()
    val nmbrOfDestinationCoutntries = dataDF.select("DEST_COUNTRY_NAME").distinct().count()
    println(nmbrOfOriginCoutntries, nmbrOfDestinationCoutntries)

    // Print only the first 10 elements of the column "DEST_COUNTRY_NAME"
    dataDF.select("DEST_COUNTRY_NAME").show(10)

    // Print the unique values related to column "DEST_COUNTRY_NAME"
    dataDF.select("DEST_COUNTRY_NAME").distinct().show(10)

    // Add +1 to the value of column "count
    dataDF.withColumn("countPlus1",$"count"+1).show(10)

    // Print top 5 destination using only scala code
    dataDF.groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort($"destination_total".desc)
      .show(5)

    // Print top 5 destination using SQL code
    dataDF.createOrReplaceTempView("myDataView")
    spark.sql(
      "SELECT DEST_COUNTRY_NAME , sum(count) as destination_total " +
        "FROM myDataView " +
        "GROUP BY DEST_COUNTRY_NAME " +
        "ORDER BY sum(count) " +
        "DESC LIMIT 5").show()

    // Print the destination for which the sum(count) is greater that 1000
    dataDF.groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort($"destination_total".desc)
      .filter($"sum(count)">1000)
      .show()

  }
}
