organization  := "com.cloudant.spark"

name := "spark-sql"

version       := "0.1-SNAPSHOT"

scalaVersion  := "2.10.4"

fork in run := true

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= {
  val sparkV =  "1.5.1"
  val sprayV = "1.3.2"
  val playJsonV = "2.2.3"
  Seq(
    "org.apache.spark"    %%  "spark-core"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-sql"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-streaming"   %  sparkV % "provided",
    "io.spray"            %%  "spray-client"  %  sprayV,
    "io.spray"            %%  "spray-can"  %  sprayV,
    "com.typesafe.play"   %%  "play-json"     %  playJsonV
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
