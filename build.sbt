name := "spark-skew-join-bench"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-kubernetes" % "3.0.1"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"

libraryDependencies += "com.typesafe" % "config" % "1.4.1"

