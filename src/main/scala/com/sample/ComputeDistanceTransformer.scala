package com.sample

import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ComputeDistanceTransformer extends CustomDfTransformer {
  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    val calculateDistanceInKilometerUdf = udf(calculateDistanceInKilometer)

    df.withColumn("distance",
      calculateDistanceInKilometerUdf(col("dep_latitude_deg"),col("dep_longitude_deg"),col("arr_latitude_deg"), col("arr_longitude_deg")))
      .withColumn("could_be_done_by_rail", col("distance") < 500)

  }

  def calculateDistanceInKilometer: (Double, Double, Double, Double) => Double = (depLat: Double, depLng: Double, arrLat: Double, arrLng: Double) => {
    val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    val latDistance = Math.toRadians(depLat - arrLat)
    val lngDistance = Math.toRadians(depLng - arrLng)
    val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(depLat)) * Math.cos(Math.toRadians(arrLat)) * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    AVERAGE_RADIUS_OF_EARTH_KM * c
  }
}
