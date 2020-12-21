package an004005.join
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaltJoin extends JoinStrategy {

  override def join(spark: SparkSession, dfLarge: DataFrame, dfMedium: DataFrame): DataFrame = {


  }
}
