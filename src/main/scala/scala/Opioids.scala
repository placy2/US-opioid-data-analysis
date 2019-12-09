package scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import swiftvis2.plotting._
import swiftvis2.plotting.styles.BarStyle.DataAndColor
import swiftvis2.plotting.renderer.SwingRenderer
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.sql.Row

object Opioids {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("spark://pandora00:7077")
      .appName("Opioid Distribution Analysis")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val buyerAnnualData = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/data/BigData/students/placy/arcos-api/data/buyer_annual.csv")

    val buyerMonthlyData = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/data/BigData/students/placy/arcos-api/data/buyerConcatMonthly.csv")


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
      .csv(
        "/data/BigData/students/placy/arcos-api/data/detail_list_reporters.csv"
      )

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
      .csv(
        "/data/BigData/students/placy/arcos-api/data/pop_counties_20062012.csv"
      )

    val statePopulations = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(
        "/data/BigData/students/placy/arcos-api/data/pop_states_20062012.csv"
      )

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
      .csv("/data/BigData/bls/la/la.area")
      .filter('areaType === "F" || 'areaType === "A")

    val blsStateData = (spark.read
      .schema(stateSchema)
      .option("header", "true")
      .option("delimeter", "\t")
      .csv("/data/BigData/bls/la/la.data.concatenatedStateFiles"))
      .filter('year > 2005 && 'year < 2012)

//-----------------------------------------------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------
//-----------------------------------------------------------------------------------------------------
    //Examining basic stats and totals
    //buyerAnnualData.agg(sum("DOSAGE_UNIT")).show()
    // 7.663060302123816E10 total pills

    //buyerMonthly12.agg(sum("DOSAGE_UNIT").alias("sum").as[Int]).select('sum).show()
    // 12,663,969,567 pills in 2012
    //buyerMonthly06.agg(sum("DOSAGE_UNIT").alias("sum").as[Int]).select('sum).show()
    // 8,389,698,373 pills in 2006

    //Comparing per capita pill purchases at the state level for all years
    // val renamedStatePop =
    //   statePopulations.select('BUYER_STATE.as("state"), 'year.as("popYear"), 'population)

    // val stateJoined = renamedStatePop
    //   .join(buyerAnnualData)
    //   .where('state === 'BUYER_STATE && 'popYear === 'year)
    // //.show(50, false)

    // val stateGrouped = stateJoined.select(
    //   'state.as[String],
    //   'popYear.as[Int],
    //   'DOSAGE_UNIT.as[Double],
    //   'population.as[Double]
    // ).groupBy('state).agg(Map(
    //   "DOSAGE_UNIT" -> "sum",
    //   "population" -> "avg",
    // ))
    
    // val statePerCap = stateGrouped.withColumn("pillsPerCap", ($"sum(DOSAGE_UNIT)") / $"avg(population)").orderBy(desc("pillsPerCap"))//.show(50, false)

    // val perCapPlotData = statePerCap.select('state.as[String], 'pillsPerCap.as[Double]).collect()
    // val perCapPlot = Plot.barPlot(perCapPlotData.map(_._1), Seq(
    //   DataAndColor(perCapPlotData.map(_._2),  BlackARGB)), false, 0.8, "Oxycodone & Hydrocodone distributions in U.S.A.", "States", "Pills per capita")

    // SwingRenderer(perCapPlot, 1200, 800, true)

  // Using Linear Regression within the dataset
    val renamedLatLon = pharmacyLatLon.select('BUYER_DEA_NO.as("code"), 'lat, 'lon)
    val joinedLatLon = renamedLatLon.join(buyerMonthlyData).where('code === 'BUYER_DEA_NO).describe().show()


    spark.sparkContext.stop()
    println("Application finished.")
  }
}
