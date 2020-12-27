package an004005.generator

import an004005.common.Config
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

object UniformDataGenerator extends DataGenerator {


  def buildTestset(spark: SparkSession,
                   numberOfKeys: Int = Config.numberOfKeys,
                   keysMultiplier: Int = Config.keysMultiplier,
                   numberOfPartitions: Int = Config.numberOfPartitions): Unit = {

    import spark.implicits._

    val numRows = numberOfRows(numberOfKeys, keysMultiplier)

    println(s"Generating $numRows rows")

    val df = spark
      .range(keysMultiplier)
      .repartition(numberOfPartitions)
      .mapPartitions(rows => {
        val r = new Random()
        val count = numRows / keysMultiplier
        rows.flatMap(_ => (0 until count.toInt)
          .map(_ => r.nextInt(numberOfKeys)))
      })
      .map(Key)
      .repartition(numberOfPartitions)

    assert(df.count() == numberOfRows())

    df
      .write
      .mode(SaveMode.Overwrite)
      .save(Config.getLargeTableName("uniform"))

    createRightTable(spark, Config.getMediumTableName("uniform"), numberOfPartitions)
  }

  def getName: String = "UniformDataGenerator"

  def getMediumTableName: String = Config.getMediumTableName("uniform")

  def getLargeTableName: String = Config.getLargeTableName("uniform")

  case class Key(key: Int)

  case class KeyLabel(key: Int, label: String)

}
