package an004005

import org.apache.spark.sql.SparkSession

object RunBenchMark extends App {
  def getSparkSession(appName: String = "spark skew join benchmark"): SparkSession =
    SparkSession
      .builder
      .appName(appName)
      .config("spark.master", "k8s://https://kubernetes.docker.internal:6443")
      .config("spark.submit.deployMode", "client")
      .config("spark.kubernetes.container.image", "spark:0.1")
      .config("spark.driver.host", "192.168.35.95")
      .config("spark.executor.instances", "1")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("parquet.enable.dictionary", "false")
      .getOrCreate()


  val spark = getSparkSession()
  spark.sql("select 1").show

}
