organization  := "mytest"

name :=  "spark_test"

version       := "0.1-SNAPSHOT"

scalaVersion  := "2.10.4"

fork in run := true

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= {
  val sparkV =  "1.3.1"
  val hadoopV = "2.4.0"
  Seq(
    "org.apache.spark"    %%  "spark-core"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-sql"	  %  sparkV % "provided",
    "org.apache.hadoop"   %   "hadoop-client" %  hadoopV % "provided"
  )
}

