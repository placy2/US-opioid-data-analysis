package scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Opioids {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")//"spark://pandora00:7077")
      .appName("Opioid Distribution Analysis")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val buyerAnnualData = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/data/BigData/students/placy/arcos-api/data/buyer_annual.csv")

    val buyerMonthly06 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/buyer_monthly2006.csv")

    val buyerMonthly07 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/buyer_monthly2007.csv")

    val buyerMonthly08 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/buyer_monthly2008.csv")

    val buyerMonthly09 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/buyer_monthly2009.csv")

    val buyerMonthly10 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/buyer_monthly2010.csv")

    val buyerMonthly11 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/buyer_monthly2011.csv")

    val buyerMonthly12 = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/buyer_monthly2012.csv")

    val countyAnnualData = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/county_annual.csv")
    
    val countyMonthlyData = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/county_monthly.csv")

    val buyerDetailData = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/detail_list_buyers.csv")

    val reporterDetailData = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/detail_list_reporters.csv")

    val pharmacyDetailData = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/pharmacies_cbsa.csv")
    
    val pharmacyLatLon = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/pharmacies_latlon.csv")

    val countyPopulations = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/pop_counties_20062012.csv")

    val statePopulations = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/BigData/students/placy/arcos-api/data/pop_states_20062012.csv")

    val stateSchema = StructType(
      Array(
        StructField("id", StringType),
        StructField("year", IntegerType),
        StructField("period", StringType),
        StructField("value", DoubleType)
      )
    )

    val areaSchema = StructType(
      Array(
        StructField("areaType", StringType),
        StructField("areaCode", StringType),
        StructField("areaName", StringType)
      )
    )

    val blsAreaData = spark.read
    .schema(areaSchema)
    .option("header", "true")
    .option("delimeter", "\t")
    .csv("/data/BigData/bls/la/la.area").filter('areaType === "F" || 'areaType === "A")

    val blsStateData = (spark.read
    .schema(stateSchema)
    .option("header", "true")
    .option("delimeter", "\t")
    .csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")).filter('year > 2005 && 'year < 2012)

//-----------------------------------------------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------
    //Examining basic stats and totals
    buyerAnnualData.agg(sum("DOSAGE_UNIT")).show()
      // 7.663060302123816E10 total pills
    
    println(buyerMonthly12.agg(sum("DOSAGE_UNIT")).collect()(0) - buyerMonthly06.agg(sum("DOSAGE_UNIT")).collect()(0))






    spark.sparkContext.stop()
    println("Application finished.")
  }
}