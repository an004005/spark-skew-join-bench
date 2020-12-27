package an004005

import an004005.common.Config
import an004005.generator.{DataGenerator, SkewedDataGenerator}
import an004005.join.{AQEJoin, AQEJoinType, BroadcastJoin, BroadcastJoinType, JoinType, NormalJoin, PartialBroadcastJoin, PartialBroadcastJoinType, SortMergeJoinType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object RunBenchMark extends App {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000 / 1000 / 1000 + " sec")
    result
  }


  def runTest(generator: DataGenerator,
              joinType: JoinType,
              tableNameOutput: String) {

    val rows = generator.numberOfRows()

    val name = s"${generator.getName}: $joinType, keys=${Config.numberOfKeys}, multiplier=${Config.keysMultiplier}, rows=$rows"

    println(name)


    val spark = getSparkSession(name)

    time {

      val out = joinType match {
        case _: SortMergeJoinType => NormalJoin.join(
          spark,
          spark
            .read
            .load(generator.getLargeTableName),
          spark
            .read
            .load(generator.getMediumTableName)
        )
        case _: BroadcastJoinType => BroadcastJoin.join(
          spark,
          spark
            .read
            .load(generator.getLargeTableName),
          spark
            .read
            .load(generator.getMediumTableName)
        )
        case _: PartialBroadcastJoinType => PartialBroadcastJoin.join(
          spark,
          spark
            .read
            .load(generator.getLargeTableName),
          spark
            .read
            .load(generator.getMediumTableName)
        )
        case _: AQEJoinType => AQEJoin.join(
          spark,
          spark
            .read
            .load(generator.getLargeTableName),
          spark
            .read
            .load(generator.getMediumTableName)
        )
      }
//      out.explain(extended = true)

      out.write
        .mode(SaveMode.Overwrite)
        .parquet(tableNameOutput)
    }

    spark.stop()
  }


  def runBenchmark(dataGenerator: DataGenerator,
                   iterations: Int = 1,
                   outputTable: String = "result.parquet"): Unit = {
    val originalMultiplier = Config.keysMultiplier

    (1 to iterations)
      .map(step => originalMultiplier * step)
      .foreach(multiplier => {

        val keys = Config.numberOfKeys
        Config.keysMultiplier = multiplier

        // Generate uniform data and benchmark
        val rows = dataGenerator.numberOfRows()

        val spark = getSparkSession(s"${dataGenerator.getName}: Generate dataset with $keys keys, $rows rows")
        dataGenerator.buildTestset(
          spark,
          keysMultiplier = multiplier
        )
        spark.stop()

//        runTest(
//          dataGenerator,
//          new BroadcastJoinType,
//          outputTable
//        )

        runTest(
          dataGenerator,
          new PartialBroadcastJoinType,
          outputTable
        )

        runTest(
          dataGenerator,
          new SortMergeJoinType,
          outputTable
        )



        runTest(
          dataGenerator,
          new AQEJoinType,
          outputTable
        )
      })

    // Reset global Config
    Config.keysMultiplier = originalMultiplier
  }

//  def getSparkSession(appName: String = "spark skew join benchmark"): SparkSession = {
//    val spark = SparkSession
//      .builder
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("parquet.enable.dictionary", "false")
//      .appName(appName)
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("WARN")
//    spark
//  }


  def getSparkSession(appName: String = "spark skew join benchmark"): SparkSession = {
    val spark = SparkSession
        .builder
        .appName(appName)
        .config("spark.master", "local[5]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("parquet.enable.dictionary", "false")
        .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

    spark.sparkContext.setLogLevel("WARN")
    spark
  }


  System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.1" )


//  runBenchmark(MixedDataGenerator)
  runBenchmark(SkewedDataGenerator)
}
// jar cvf my.jar an004005
