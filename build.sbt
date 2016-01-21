name := "Beleg3_SimilarityAnalysis"

version := "0.1"

scalaVersion := "2.10.5"

javacOptions ++= Seq("-source", "1.7", "-target", "1.8")

scalacOptions ++= Seq("-target:jvm-1.7")

parallelExecution in Test := false

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "1.5.1",
			   "org.apache.spark" %% "spark-sql" % "1.5.1",
			   "org.jfree" % "jfreechart" % "1.0.19","junit" % "junit" % "4.11",
			   "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"	)


