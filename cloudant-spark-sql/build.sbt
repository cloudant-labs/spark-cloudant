organization  := "cloudant-labs"

name := "spark-cloudant"

version       := "1.6.3"

scalaVersion  := "2.10.4"

fork in run := true

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= {
  val sparkV =  "1.6.0"
  val sprayV = "1.3.2"
  val playJsonV = "2.2.3"
  val httpcomponentsV = "4.5.2"
  Seq(
    "org.apache.spark"    %%  "spark-core"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-sql"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-streaming"   %  sparkV % "provided",
    "io.spray"            %%  "spray-client"  %  sprayV,
    "io.spray"            %%  "spray-can"  %  sprayV,
    "com.typesafe.play"   %%  "play-json"     %  playJsonV,
    "org.apache.httpcomponents" % "httpclient" % httpcomponentsV
  )
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case "reference.conf"   => MergeStrategy.first
  case PathList("scala", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName in assembly := "cloudant-spark.jar"


spAppendScalaVersion := true

spName := "cloudant-labs/spark-cloudant"

sparkVersion  := "1.6.0"

sparkComponents := Seq("sql")

spShortDescription := "Spark SQL Cloudant External Datasource"

spDescription := """Spark SQL Cloudant External Datasource.
                   |Cloudant integration with Spark as Spark SQL external datasource.
                   | - Allows to load data from Cloudant dabases and indexes into Spark.
                   | - Allows to save resulting data from Spark to existing Cloudant databases.
                   | - Supports predicates push down (only based on _id field in databases, but varios fields for indexes).
                   | - Support column pruning for indexes.""".stripMargin

spAppendScalaVersion := true

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
