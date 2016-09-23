organization  := "examples"

name :=  "spark_test"

version       := "2.0.0"

scalaVersion  := "2.11.8"

fork in run := true

resolvers ++= Seq(
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= {
  val sparkV =  "2.0.0"
  Seq(
    "org.apache.spark"    %%  "spark-core"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-sql"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-streaming"   %  sparkV % "provided"
  )
}

sparkVersion  := "2.0.0"
