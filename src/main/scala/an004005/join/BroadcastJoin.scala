package an004005.join

import org.apache.spark.sql.{DataFrame, SparkSession}


object BroadcastJoin extends JoinStrategy {

  override def join(spark: SparkSession,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {
    spark.conf.set("spark.sql.adaptive.enabled", value = false)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760 )

    dfLarge
      .join(
        dfMedium.hint("broadcast"),
        Seq("key"),
        "left_outer"
      )
      .select(
        dfLarge("key"),
        dfMedium("label")
      )
  }

}
