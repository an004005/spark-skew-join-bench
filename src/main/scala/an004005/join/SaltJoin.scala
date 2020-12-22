package an004005.join
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaltJoin extends JoinStrategy {

  override def join(spark: SparkSession, dfLarge: DataFrame, dfMedium: DataFrame): DataFrame = {
    spark.conf.set("spark.sql.adaptive.enabled", value = false)

    import org.apache.spark.sql.functions._

    dfLarge
      .join(dfMedium, Seq("key"), "left_outer"
      )
      .select(
        dfLarge("key"),
        dfMedium("label")
      )
  }
}
