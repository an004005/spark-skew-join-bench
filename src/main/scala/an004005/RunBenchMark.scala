package an004005

import an004005.common.Config
import an004005.generator.{DataGenerator, MixedDataGenerator, SkewedDataGenerator}
import an004005.join.{IterativeBroadcastJoin, IterativeBroadcastJoinType, JoinType, NormalJoin, PartialIterativeBroadcastJoin, PartialIterativeBroadcastJoinType, SortMergeJoinType}
import org.apache.spark.sql.types.IntegerType
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

    val name = s"${generator.getName}: $joinType, passes=${Config.numberOfBroadcastPasses}, keys=${Config.numberOfKeys}, multiplier=${Config.keysMultiplier}, rows=$rows"

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
        case _: IterativeBroadcastJoinType => IterativeBroadcastJoin.join(
          spark,
          spark
            .read
            .load(generator.getLargeTableName),
          spark
            .read
            .load(generator.getMediumTableName)
        )
        case _: PartialIterativeBroadcastJoinType => PartialIterativeBroadcastJoin.join(
          spark,
          spark
            .read
            .load(generator.getLargeTableName),
          spark
            .read
            .load(generator.getMediumTableName)
        )
      }

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

        Config.numberOfBroadcastPasses = 2

        runTest(
          dataGenerator,
          new IterativeBroadcastJoinType,
          outputTable
        )

//        runTest(
//          dataGenerator,
//          new PartialIterativeBroadcastJoinType,
//          outputTable
//        )

        runTest(
          dataGenerator,
          new SortMergeJoinType,
          outputTable
        )

//        Config.numberOfBroadcastPasses = 3
//
//        runTest(
//          dataGenerator,
//          new IterativeBroadcastJoinType,
//          outputTable
//        )
//
//        runTest(
//          dataGenerator,
//          new PartialIterativeBroadcastJoinType,
//          outputTable
//        )
//
//        runTest(
//          dataGenerator,
//          new SortMergeJoinType,
//          outputTable
//        )
      })

    // Reset global Config
    Config.keysMultiplier = originalMultiplier
  }

//  def getSparkSession(appName: String = "spark skew join benchmark"): SparkSession = {
//    val spark = SparkSession
//      .builder
//      .appName(appName)
//      .config("spark.master", "k8s://https://kubernetes.docker.internal:6443")
//      .config("spark.submit.deployMode", "client")
//      .config("spark.kubernetes.container.image", "spark:0.1")
//      .config("spark.driver.host", "192.168.35.95")
//      .config("spark.executor.instances", "3")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("parquet.enable.dictionary", "false")
//      .getOrCreate()
//
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
//    spark.sparkContext.setLogLevel("WARN")
//    spark
//  }

  def getSparkSession(appName: String = "spark skew join benchmark"): SparkSession = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("parquet.enable.dictionary", "false")
    spark.sparkContext.setLogLevel("WARN")
    spark
  }


//  def getSparkSession(appName: String = "spark skew join benchmark"): SparkSession = {
//    val spark = SparkSession
//      .builder
//      .appName(appName)
//      .config("spark.master", "local[5]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("parquet.enable.dictionary", "false")
//      .getOrCreate()
//
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
//    spark.sparkContext.setLogLevel("WARN")
//    spark
//  }


  System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.1" )


//  runBenchmark(MixedDataGenerator)
  runBenchmark(SkewedDataGenerator)
}
// jar cvf my.jar an004005
