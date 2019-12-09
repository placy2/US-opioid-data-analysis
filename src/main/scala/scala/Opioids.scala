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
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression

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
        StructField("stateYear", IntegerType),
        StructField("period", StringType),
        StructField("value", DoubleType)
      )
    )

    val areaSchema = StructType(
      Array(
        StructField("areaType", StringType),
        StructField("areaCode", StringType),
        StructField("areaName", StringType)
        // StructField("displayLevel", StringType),
        // StructField("selectable", StringType),
        // StructField("sort_sequence", IntegerType)
      )
    )

    val blsAreaData = spark.read
      .schema(areaSchema)
      .option("header", "true")
      .option("delimiter", "\t")
      .csv("/data/BigData/bls/la/la.area")

    val blsStateData = (spark.read
      .schema(stateSchema)
      .option("header", "true")
      .option("delimiter", "\t")
      .csv("/data/BigData/bls/la/la.data.concatenatedStateFiles"))
      .filter('stateYear > 2005 && 'stateYear < 2012)

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
  
    val evaluator = new RegressionEvaluator().setLabelCol("DOSAGE_UNIT").setPredictionCol("prediction").setMetricName("rmse")

    // val renamedLatLon = pharmacyLatLon.select('BUYER_DEA_NO.as("code"), 'lat, 'lon)
    // val joinedLatLon = renamedLatLon.join(buyerMonthlyData).where('code === 'BUYER_DEA_NO)//.describe().show()

    // val llVA = new VectorAssembler().setInputCols(Array("lat", "lon")).setOutputCol("llVect")
    // val latLonRegVect = llVA.transform(joinedLatLon.na.drop(Seq("DOSAGE_UNIT", "lat", "lon")))

    // val latLonLR = new LinearRegression().setFeaturesCol("llVect").setLabelCol("DOSAGE_UNIT")
    // val latLonLRModel = latLonLR.fit(latLonRegVect)
    // val predictions = latLonLRModel.transform(latLonRegVect)

    // println("Average error: " + evaluator.evaluate(predictions))
    // 16,547 with lat and lon
  
  // Using Linear Regression with unemployment
    val unempCounties = blsAreaData.where('areaType === "F").withColumn("upperCounty", upper('areaName))
    val joinedUnempCounties = countyMonthlyData.join(unempCounties).filter('upperCounty.contains('BUYER_COUNTY))//.show(5, false)
    val bigJoinedUnemp = joinedUnempCounties.join(blsStateData).where('id.contains('areaCode) && 'year === 'stateYear)
    
    //bigJoinedUnemp.printSchema()

    val smallerPop = countyPopulations.select('BUYER_COUNTY.as("county"), 'STATE.as("stateNum"), 'year.as("countyYear"), 'population)
    val popJoinedUnemp = smallerPop.join(bigJoinedUnemp).filter('county === 'BUYER_COUNTY && 'year === 'countyYear)
    popJoinedUnemp.printSchema()

    val unempVA = new VectorAssembler().setInputCols(Array(""))




    











    // val renamedBuyerMonthly = buyerMonthlyData.select('BUYER_DEA_NO.as("code"), 'DOSAGE_UNIT.as("pills").as[Double], 'month)
    // val joinedDetails = renamedBuyerMonthly.join(buyerDetailData).where('code === 'BUYER_DEA_NO)

    // val detailVA = new VectorAssembler().setInputCols(Array("BUYER_BUS_ACT", "BUYER_ZIP", "month")).setOutputCol("detailVect")
    // val detailRegVect = detailVA.transform(joinedDetails.na.drop(Seq("BUYER_BUS_ACT", "BUYER_CITY", "BUYER_STATE", "BUYER_ZIP", "month", "pills")))

    // val detailLR = new LinearRegression().setFeaturesCol("detailVect").setLabelCol("pills")
    // val detailLRModel = detailLR.fit(detailRegVect)
    // val predictions = detailLRModel.transform(detailRegVect)

    // println("Average error: " + evaluator.evaluate(predictions))

    spark.sparkContext.stop()
    println("Application finished.")
  }
}
