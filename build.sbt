name := "TrendingRecommendationsJob"

version := "0.1"

val sparkVersion = "3.2.2"
val vegasVersion = "0.3.11"
val CirceVersion = "0.14.1"

scalaVersion := "2.12.10"


resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.plotly-scala" %% "plotly-render" % "0.8.2",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.18.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.18.0",
  "io.jvm.uuid" %% "scala-uuid" % "0.3.1"
)

libraryDependencies ++= List(
  "io.circe" %% "circe-core" % CirceVersion,
  "io.circe" %% "circe-generic" % CirceVersion,
  "io.circe" %% "circe-parser" % CirceVersion
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % "test"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

