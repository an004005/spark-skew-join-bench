package an004005.common

import com.typesafe.config.{ConfigFactory}

object Config {
  private val conf = ConfigFactory.load

  var broadcastIterationTableName: String = "tmp_broadcast_table.parquet"

  // The number of partitions
  var numberOfPartitions: Int = conf.getInt("generator.partitions") // 200

  // The number of rows
  var numberOfKeys: Int = conf.getInt("generator.keys") // 100000

  // The number of times the keys get duplicated,
  // This controls the skewness
  var keysMultiplier: Int = conf.getInt("generator.multiplier") // 1000

  def getMediumTableName(generatorType: String): String = {
    conf.getString("generator.mediumRight")
  }

  def getLargeTableName(generatorType: String): String = {
    conf.getString("generator.largeLeft")
  }

  val getLargeRightTableName: String = conf.getString("generator.largeRight")

  val getRightSize: String = conf.getString("generator.rightSize")
}
