lazy val cloudant_spark_sql = project.in(file("cloudant-spark-sql"))

lazy val examples = project.in(file("examples/scala")).dependsOn(cloudant_spark_sql)
