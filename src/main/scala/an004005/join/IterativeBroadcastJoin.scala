package an004005.join

import an004005.common.{Config, SparkUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec

object IterativeBroadcastJoin extends JoinStrategy {

  @tailrec
  private def iterativeBroadcastJoin(spark: SparkSession,
                                     result: DataFrame,
                                     broadcast: DataFrame,
                                     iteration: Int = 0): DataFrame =
    if (iteration < Config.numberOfBroadcastPasses) {

      val out = result.join(
        broadcast.filter(col("pass") === lit(iteration)),
        Seq("key"),
        "left_outer"
      ).select(
        result("key"),

        // Join in the label
        coalesce(
          result("label"),
          broadcast("label")
        ).as("label")
      )

      iterativeBroadcastJoin(
        spark,
        out,
        broadcast,
        iteration + 1
      )
    } else result

  override def join(spark: SparkSession,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {
    spark.conf.set("spark.sql.adaptive.enabled", value = false)

    broadcast(dfMedium)
    iterativeBroadcastJoin(
      spark,
      dfLarge
        .select("key")
        .withColumn("label", lit(null)),
      dfMedium
    )
  }

}
