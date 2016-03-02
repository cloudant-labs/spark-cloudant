organization  := "examples"

name :=  "spark_test"

version       := "0.1-SNAPSHOT"

scalaVersion  := "2.10.4"

fork in run := true

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= {
  val sparkV =  "1.5.1"
  Seq(
    "org.apache.spark"    %%  "spark-core"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-sql"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-streaming"   %  sparkV % "provided"
  )
}

