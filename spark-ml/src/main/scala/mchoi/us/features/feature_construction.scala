package mchoi.us.features

import mchoi.us.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object feature_construction {

  /**
    * The format of feature should be looked like ((patient-id, feature-name), feature-value)
    * feature-value : Aggregate feature tuples from diagnostic with COUNT aggregation
    */

  type FeatureTuple = ((String, String), Double)


  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {

    diagnostic.map(f => ((f.patientID, f.code), 1.0)).keyBy(f => f._1).reduceByKey((x, y) => (x._1, x._2 + y._2)).map(f => f._2)}

    // val diag_feature = diagnostic.map(x => ((x.patientID, x.code), 1.0)).keyBy(k => k._1).reduceByKey((x,y) => (x._1, x._2 + y._2)).map(f => f._2)
    // diag_feature.collect()

  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {

    medication.map(f => ((f.patientID, f.medicine), 1.0)).keyBy(f => f._1).reduceByKey((x, y) => (x._1, x._2 + y._2)).map(f => f._2)}

    // val med_feature = medication.map(x => ((x.patientID, x.medicine), 1.0)).keyBy(k => k._1).reduceByKey((x,y) => (x._1, x._2 + y._2)).map(f => f._2)
    // med_feature.collect()

  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
      * TODO implement your own code here and remove existing
      * placeholder code
      */
    labResult.map(f => ((f.patientID, f.testName), f.value, 1)).keyBy(_._1).reduceByKey((x, y) => (x._1, x._2 + y._2, x._3 + y._3)).
      map(f => (f._1, f._2._2 / f._2._3))

    // val lab_feature = labResult.map(x => (x.patientID, x.testName, x.value, 1)).keyBy(k => k._1)
    // lab_feature.collect()
  }

  /**
    * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
    * available in the given set only and drop all others.
    *
    * @param diagnostic   RDD of diagnostics
    * @param candiateCode set of candidate code, filter diagnostics based on this set
    * @return RDD of feature tuples
    */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
      * TODO implement your own code here and remove existing
      * placeholder code
      */
    constructDiagnosticFeatureTuple(diagnostic.filter(f => candiateCode.contains(f.code.toLowerCase())))
  }

  /**
    * Aggregate feature tuples from medication with COUNT aggregation, use medications from
    * given set only and drop all others.
    *
    * @param medication          RDD of diagnostics
    * @param candidateMedication set of candidate medication
    * @return RDD of feature tuples
    */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
      * TODO implement your own code here and remove existing
      * placeholder code
      */
    constructMedicationFeatureTuple(medication.filter(f => candidateMedication.contains(f.medicine.toLowerCase())))
  }

  /**
    * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
    * given set of lab test names only and drop all others.
    *
    * @param labResult    RDD of lab result
    * @param candidateLab set of candidate lab test name
    * @return RDD of feature tuples
    */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
      * TODO implement your own code here and remove existing
      * placeholder code
      */
    constructLabFeatureTuple(labResult.filter(f => candidateLab.contains(f.testName.toLowerCase())))
  }

  /**
    * Given a feature tuples RDD, construct features in vector
    * format for each patient. feature name should be mapped
    * to some index and convert to sparse feature format.
    *
    * @param sc      SparkContext to run
    * @param feature RDD of input feature tuples
    * @return
    */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map */

    val f_map = feature.map(f => f._1._2).distinct().zipWithIndex().collectAsMap()
    val id_map = sc.broadcast(f_map)
    val id_num = id_map.value.size

    /** transform input feature */
    val result = feature.map(f => (f._1._1, id_map.value(f._1._2), f._2)).groupBy(_._1).map(f => {
      val featurelist = f._2.toList.map(x => (x._2.toInt, x._3))
      (f._1, Vectors.sparse(id_num, featurelist))
    })
    result
  }
}


/**
      * Functions maybe helpful:
      * collect
      * groupByKey
      */

    /**
      * TODO implement your own code here and remove existing
      * placeholder code
      *
      * val result = sc.parallelize(Seq(
      * ("Patient-NO-1", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-2", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-3", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-4", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-5", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-6", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-7", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-8", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-9", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
      * ("Patient-NO-10", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))))
      * result
      */
    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */


/**
val diag = diagnostic.map(f => ((f.patientID, f.code), 1.0)).keyBy(f => f._1).reduceByKey((x, y) => (x._1, x._2 + y._2)).map(f => f._2)
val featureMap = diag.map(f => f._1._2).distinct().zipWithIndex().collectAsMap()

val scFeatureMap = sc.broadcast(featureMap)
val finalSample = diag.map {case(patient_id, code) =>
  val numFeature = scFeatureMap.value.size
  val indexedFeatures = code.toList.
  map{case(featureName, featureValue) => (scFeatureMap.value(featureName), featureValue)}

  val featureVector = Vectors.sparse(numFeature, indexedFeatures)
  val labeledPoint = LabeledPoint(patient_id, featureVector)
  LabeledPoint
}

*

}
  */
lab_feature.count()