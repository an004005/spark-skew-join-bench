package an004005.generator

import an004005.common.Config
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

object MixedDataGenerator extends DataGenerator {

  def buildTestset(spark: SparkSession,
                   numberOfKeys: Int = Config.numberOfKeys,
                   keysMultiplier: Int = Config.keysMultiplier,
                   numberOfPartitions: Int = Config.numberOfPartitions): Unit = {

    import spark.implicits._

    val numRows = numberOfRows(numberOfKeys, keysMultiplier)
    println(s"Generating ${numRows * 2} rows")

    val skewMultiplier = (keysMultiplier * 0.7).asInstanceOf[Int]
    val dfSkew = spark
      .sparkContext
      .parallelize(generateSkewedSequence(numberOfKeys), numberOfPartitions)
      .flatMap(list => (0 until skewMultiplier).map(_ => list))
      .repartition(numberOfPartitions)
      .flatMap(pair => skewDistribution(pair._1, pair._2))
      .toDS()
      .map(Key)
      .repartition(numberOfPartitions)

    val uniformMultiplier = (keysMultiplier * 0.3).asInstanceOf[Int]
    val dfUniform = spark
      .range(uniformMultiplier)
      .repartition(numberOfPartitions)
      .mapPartitions(rows => {
        val r = new Random()
        val count = numRows / uniformMultiplier
        rows.flatMap(_ => (0 until count.toInt)
          .map(_ => r.nextInt(numberOfKeys)))
      })
      .map(Key)
      .repartition(numberOfPartitions)

    val df = dfSkew.union(dfUniform)
      .repartition(numberOfPartitions)

    df
      .write
      .mode(SaveMode.Overwrite)
      .save(Config.getLargeTableName("mixed"))

    createMediumTable(spark, Config.getMediumTableName("mixed"), numberOfPartitions)
  }

  /**
    * Will generate a sequence of the input sample
    *
    * @param key   The sample and the
    * @param count count the number of repetitions
    * @return
    */
  def skewDistribution(key: Int, count: Int): Seq[Int] = Seq.fill(count)(key)

  def getName: String = "MixedDataGenerator"

  def getMediumTableName: String = Config.getMediumTableName("mixed")

  def getLargeTableName: String = Config.getLargeTableName("mixed")

  case class Key(key: Int)

  case class KeyLabel(key: Int, label: String, pass: Int)
}
