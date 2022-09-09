package trending.plotter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format}
import plotly.element.ScatterMode
import plotly.layout.{Axis, Layout}
import plotly.{Plotly, Scatter}
import trending.domain._
import trending.util.FilesManager

import java.io.File
import java.sql.Timestamp

object PlottingDataFrame {

  def plotDataFrame(dataFrame: DataFrame): File = {
    FilesManager.deleteFile("div(.+).html")
    Plotly.plot("div-id.html", generatePlottingScatters(dataFrame), generateLayout)
  }

  def generatePlottingScatters(dataFrame: DataFrame): List[Scatter] = {
    val frame: DataFrame = dataFrame
      .select(
        col(COMMERCE),
        col(IMSORGID),
        col(PRODUCTITEMID),
        col("counts_and_time.count") as COUNTCOL,
        date_format(col("counts_and_time.timestamp"), DATEFORMAT) as "start"
      )

    val list: List[((String, String, String), Array[(Long, Long)])] = frame
      .select(col(COMMERCE), col(PRODUCTITEMID),
        col("start"), col(COUNTCOL),
        col(IMSORGID))
      .collect()
      .map(r => (
        r.getAs[Int](PRODUCTITEMID).toString,
        r.getAs[String](COMMERCE),
        Timestamp.valueOf(r.getAs[String]("start")).getTime,
        r.getAs[Long](COUNTCOL),
        r.getAs[String](IMSORGID)))
      .groupBy(x => (x._1, x._2, x._5))
      .map(x => (x._1, x._2.map(y => (y._3, y._4))))
      .toList

    val tuplesList: List[(String, (Array[Long], Array[Long]))] =
      list.map(t => (s"${t._1._2}-${t._1._1}-${t._1._3}", t._2))
        .map(t => (t._1, t._2.sortBy(_._1)))
        .map(t => (t._1, t._2.unzip))

    tuplesList
      .map((tuple: (String, (Array[Long], Array[Long]))) => {
        val x: List[Long] = tuple._2._1.toList
        val y: List[Long] = tuple._2._2.toList
        Scatter()
          .withX(x)
          .withY(y)
          .withMode(ScatterMode(ScatterMode.Markers, ScatterMode.Lines))
          .withName(tuple._1)
      })
  }

  def generateLayout: Layout = Layout(
    title = "Counts per second"
  ).withHeight(600)
    .withXaxis(Axis().withTitle("Seconds"))
    .withYaxis(Axis().withTitle("Counts"))
}
