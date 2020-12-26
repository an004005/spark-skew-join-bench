package an004005.join
import an004005.common.{Config, SparkUtil}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * large table join large table optimization
 * 방법: 두개의 큰 테이블을 조인하는데 skew데이터와 uniform데이터가 섞여 있는 데이터를 가정
 * 이때 skew된 데이터는 broadcast로 join한다.
 * skew데이터의 양이 많으면 iterative broadcast join한다.
 * 나머지 uniform데이터는 sort merge조인으로 처리한다.
 */
object PartialIterativeBroadcastJoin extends JoinStrategy {
  import org.apache.spark.sql.functions._

  case class splitDataFrame(uniform: DataFrame, skew: DataFrame)

  override def join(spark: SparkSession,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.adaptive.enabled", value = false)

    val isSkewTable = getIsSkewTable(dfLarge)

    val largePair = splitSkewAndUniform(dfLarge, isSkewTable, "large")
    val largeUniform = SparkUtil.dfRead(spark, largePair._1)
    val largeSkew = SparkUtil.dfRead(spark, largePair._2)


    val mediumPair = splitSkewAndUniform(dfMedium, isSkewTable, "medium")
    val mediumUniform = SparkUtil.dfRead(spark, mediumPair._1)
    val mediumSkew = SparkUtil.dfRead(spark, mediumPair._2)

    val dfUniform = NormalJoin.join(spark, largeUniform, mediumUniform)
    val dfSkew = IterativeBroadcastJoin.join(spark, largeSkew, mediumSkew)

    dfUniform.union(dfSkew)
  }

  // 각 key의 개수의 평균 개수, skew를 판단하는 기준으로 사용
  def getIsSkewTable(dfLarge: DataFrame): DataFrame = {
    println("make IsSkewTable")
    val temp = dfLarge.groupBy("key")
      .agg(count("*").as("count"))
    temp.persist(StorageLevel.MEMORY_ONLY)

    val keyMeanCount = temp
      .agg(mean("count").as("mean"))
      .take(1)(0).getDouble(0)
    println(s"mean key count: $keyMeanCount")

    val skewFactor = Config.skewFactor

    val out = temp.withColumn("isSkew",
      when(col("count") >= keyMeanCount * skewFactor, true)
        .otherwise(false)).drop("count")
    temp.unpersist()
    out
  }

  def splitSkewAndUniform(df: DataFrame, isSkewTable: DataFrame, name: String): (String, String) = {
    df.join(
      broadcast(isSkewTable),
      Seq("key")
    ).write
      .partitionBy("isSkew")
      .mode(SaveMode.Overwrite)
      .parquet(s"is_skew_table_$name")

    // (uniform, skew)
    (s"is_skew_table_$name/isSkew=false", s"is_skew_table_$name/isSkew=true")
  }
}
