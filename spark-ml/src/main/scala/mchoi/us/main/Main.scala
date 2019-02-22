package mchoi.us.main

import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Date

import mchoi.us.clustering.Metrics
import mchoi.us.features.feature_construction
import mchoi.us.helper.{CSVHelper, SparkHelper}
import mchoi.us.model.{Diagnostic, LabResult, Medication}
import mchoi.us.phenotyping.T2dmPhenotype
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.types.{DateType, DoubleType}

import scala.io.Source

object Main {

  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{ Level, Logger }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkHelper.spark
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    /** initialize loading of data **/
    val (medication, labResult, diagnostic) = loadRddRawData(spark)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData



    /** feature construction with all features **/

    println(medication.collect())


  }

  /********************************************************************************/
  /*
    * Load the sets of string for filtering of medication
    * lab result and diagnostics
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)

    //
  }

  def sqlDateParser(input: String, pattern: String = "yyyy-MM-dd'T'HH:mm:ssX"): java.sql.Date = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    new java.sql.Date(dateFormat.parse(input).getTime)
  }

  def loadRddRawData(spark: SparkSession): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /* the sql queries in spark required to import sparkSession.implicits._ */
    import spark.implicits._
    val sqlContext = spark.sqlContext

    /* a helper function sqlDateParser may useful here */

    /**
      * load data using Spark SQL into three RDDs and return them
      * Hint:
      * You can utilize edu.gatech.cse6250.helper.CSVHelper
      * through your sparkSession.
      *
      * This guide may helps: https://bit.ly/2xnrVnA
      *
      * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
      * Be careful when you deal with String and numbers in String type.
      * Ignore lab results with missing (empty or NaN) values when these are read in.
      * For dates, use Date_Resulted for labResults and Order_Date for medication.
      * val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
      */

    /*UDF Functions */
    sqlContext.udf.register("rm_comma", (s: String) => s.replace(",", ""))
    sqlContext.udf.register("to_lower", (s: String) => s.toLowerCase())
    sqlContext.udf.register("date_convert", (s: String) => sqlDateParser(s))

    /*importing Raw Data from HDFS */
    val meddata = CSVHelper.loadCSVAsTable(spark, "data/medication_orders_INPUT.csv", "Med")
    val labdata = CSVHelper.loadCSVAsTable(spark, "data/lab_results_INPUT.csv", "Lab")
    val encounter = CSVHelper.loadCSVAsTable(spark, "data/encounter_INPUT.csv", "enc")
    val encounter_dx = CSVHelper.loadCSVAsTable(spark, "data/encounter_dx_INPUT.csv", "enc_dx")

    //encounter.show()
    // encounter_dx.printSchema()

    /*Data Cleaning - Sql syntax - Column Selecting */
    val sql_med = sqlContext.sql("SELECT Member_ID AS patientID, Order_Date AS date, to_lower(Drug_Name) As medicine FROM Med")
    val sql_lab_a = sqlContext.sql("SELECT Member_ID AS patientID, Date_Resulted AS date, to_lower(Result_Name) AS testName, rm_comma(Numeric_Result) as value FROM Lab WHERE Numeric_Result is not null")
    val sql_lab_b = sql_lab_a.withColumn("value", $"value".cast(DoubleType))
    val sql_diag = sqlContext.sql("SELECT enc.Member_id AS patientId, enc.Encounter_DateTime AS date, enc_dx.code AS code FROM enc INNER JOIN enc_dx on enc.Encounter_ID = enc_dx.Encounter_ID")

    // sql_med.show()
    // sql_diag.printSchema()

    /*RDD Transfer from Data Frame */
    val medication: RDD[Medication] = sql_med.as[Medication].rdd
    val labResult: RDD[LabResult] = sql_lab_b.as[LabResult].rdd
    val diagnostic: RDD[Diagnostic] = sql_diag.as[Diagnostic].rdd

    // medication.collect()
    // labResult.collect()
    // diagnostic.collect()

    (medication, labResult, diagnostic)
    /*val labResultD = sql_lab.map(r => sql_lab(r._1, r._2, r._3, r._4.toString.filter(c => c != ',').toDouble))*/
  }

}
