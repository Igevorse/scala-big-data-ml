name := "scala-big-data-ml"

version := "1.0"

scalaVersion := "2.11.11"

resolvers += "Apache Staging" at "https://repository.apache.org/content/groups/staging/"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  Resolver.sonatypeRepo("releases")
)

// spark
libraryDependencies ++= {
  val sparkVersion = "2.2.0"
  val cassandraConnectorVersion = "2.0.1-s_2.11"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" % "spark-mllib_2.11" % sparkVersion,
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
    "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion
  )
}
