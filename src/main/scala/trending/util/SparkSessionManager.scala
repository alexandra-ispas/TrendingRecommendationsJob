package trending.util

import org.apache.spark.sql.SparkSession

class SparkSessionManager(val name: String) {

  val spark: SparkSession = createSparkSession(name)

  def closeSession(): Unit = spark.close()

  private def createSparkSession(name: String): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName(name)
      .config("spark.master", "local")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }
}

object SparkSessionManager {
  def apply(sessionName: String): SparkSessionManager = new SparkSessionManager(sessionName)
}