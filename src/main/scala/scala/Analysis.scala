package scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType

object Analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Opioid Distribution Analysis")
      .getOrCreate()
    try {
      import spark.implicits._

      spark.sparkContext.setLogLevel("WARN")

      val schema = StructType(
        Array(
          StructField("ReporterDEANo", StringType),
          StructField("ReporterBusAct", StringType),
          StructField("ReporterName", StringType),
          StructField("ReporterAddlCoInfo", StringType),
          StructField("ReporterAddress1", StringType),
          StructField("ReporterAddress2", StringType),
          StructField("ReporterCity", StringType),
          StructField("ReporterState", StringType),
          StructField("ReporterZip", StringType),
          StructField("ReporterCounty", StringType),

          StructField("BuyerDEANo", StringType),
          StructField("BuyerBusAct", StringType),
          StructField("BuyerName", StringType),
          StructField("BuyerAddlCoInfo", StringType),
          StructField("BuyerAddress1", StringType),
          StructField("BuyerAddress2", StringType),
          StructField("BuyerCity", StringType),
          StructField("BuyerState", StringType),
          StructField("BuyerZip", StringType),
          StructField("BuyerCounty", StringType),

          StructField("TransactionCode", StringType),
          StructField("DrugCode", StringType),
          StructField("NDCNo", StringType),
          StructField("DrugName", StringType),
          StructField("Quantity", DoubleType),
          StructField("Unit", StringType),
          StructField("ActionIndicator", StringType),   
          StructField("OrderFormNo", StringType),
          StructField("CorrectionNo", StringType),
          StructField("Strength", StringType),
          StructField("TransactionDate", StringType),
          StructField("CalcBaseWtinGrams", DoubleType),
          StructField("DosageUnit", DoubleType),
          //Below are fields created by Washington Post for cleaner data. For readable data use these!
          StructField("TransactionID", StringType),
          StructField("ProductName", StringType), 
          StructField("IngredientName", StringType),
          StructField("MeasureMMEConversionFactor", StringType),
          StructField("CombinedLabelerName", StringType),
          StructField("RevisedCompanyName", StringType),
          StructField("ReporterFamily", StringType),
          StructField("DosStr"/*Dose strength? Not sure*/, DoubleType)
        )
      )

      val opioidData = spark.read
        .schema(schema)
        .option("header", "true")
        .option("delimeter", "\t")
        .csv("/home/parker/Dependencies/arcos-co-statewide-itemized.tsv")

      opioidData.printSchema()
      opioidData.describe().show()
      // Interesting conclusions from describe:
      // 
      
      
      // DRUG AND INGREDIENT STATS
    




    //-------------------------------------------------------------------------------------------
    spark.sparkContext.stop()
    println("Application finished.")

    } catch {
      case e: org.apache.spark.SparkException => spark.stop()
      // case _: Throwable => {
      //   spark.sparkContext.stop()
      //   println("Application closed due to an error.")
      // }
    }
  }
}
