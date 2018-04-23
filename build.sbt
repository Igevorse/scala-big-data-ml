name := "scala-big-data-ml"

version := "1.0"

scalaVersion := "2.11.11"

resolvers += "Apache Staging" at "https://repository.apache.org/content/groups/staging/"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  Resolver.sonatypeRepo("releases")
)
resolvers += Classpaths.typesafeReleases

// spark
libraryDependencies ++= {
  val sparkVersion = "2.2.0"
  val cassandraConnectorVersion = "2.0.1-s_2.11"
  val ScalatraVersion = "2.6.2"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" % "spark-mllib_2.11" % sparkVersion,
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
    "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
    "org.scalatra" %% "scalatra" % ScalatraVersion,
    "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime",
    "org.eclipse.jetty" % "jetty-webapp" % "9.4.8.v20171121" ,
    "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
    "org.scalatra" %% "scalatra-json" % ScalatraVersion,
    "org.json4s"   %% "json4s-jackson" % "3.2.10",
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.5"
  )
}
