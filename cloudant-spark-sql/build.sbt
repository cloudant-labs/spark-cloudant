organization  := "cloudant-labs"

name := "spark-cloudant"

version       := "2.0.0"

scalaVersion  := "2.11.8"

fork in run := true

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)

// a 'compileonly' configuation
ivyConfigurations += config("compileonly").hide

libraryDependencies ++= {
  val sparkV =  "2.0.0"
  Seq(
    "org.apache.spark"    %%  "spark-core"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-sql"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-streaming"   %  sparkV % "provided",
    "com.typesafe.play"   %%  "play-json"     %  "2.5.9" % "provided",
    "org.scalaj" %% "scalaj-http" % "2.3.0",
    "org.slf4j" % "slf4j-api" % "1.7.21"
  )
}

// appending everything from 'compileonly' to unmanagedClasspath
unmanagedClasspath in Compile ++=
  update.value.select(configurationFilter("compileonly"))

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

sparkVersion  := "2.0.0"

sparkComponents := Seq("sql", "streaming")

spShortDescription := "Spark SQL Cloudant External Datasource"

spDescription := """Spark SQL Cloudant External Datasource.
                   |Cloudant integration with Spark as Spark SQL external datasource.
                   | - Allows to load data from Cloudant dabases and indexes into Spark.
                   | - Allows to save resulting data from Spark to existing Cloudant databases.
                   | - Supports predicates push down (only based on _id field in databases, but varios fields for indexes).
                   | - Support column pruning for indexes.""".stripMargin

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
