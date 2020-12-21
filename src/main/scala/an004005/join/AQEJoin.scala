package an004005.join
import org.apache.spark.sql.{DataFrame, SparkSession}

class AQEJoin extends JoinStrategy {
  override def join(spark: SparkSession, dfLarge: DataFrame, dfMedium: DataFrame): DataFrame = {
    spark.conf.set("spark.sql.adaptive.enabled", 1)

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
