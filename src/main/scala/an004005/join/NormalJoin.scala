package an004005.join

import org.apache.spark.sql.{DataFrame, SparkSession}

object NormalJoin extends JoinStrategy {

  override def join(spark: SparkSession, dfLarge: DataFrame, dfMedium: DataFrame): DataFrame = {
    // Explicitly disable the broadcastjoin
    spark.conf.set("spark.sql.adaptive.enabled", value = false)

    dfLarge
      .join(
        dfMedium,
        Seq("key"),
        "left_outer"
      )
      .select(
        dfLarge("key"),
        dfMedium("label")
      )
  }
}
