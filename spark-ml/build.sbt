name := "spark-ml"

version := "0.1"

scalaVersion := "2.11.8"

lazy val hadoopVersion = "2.7.5"
lazy val sparkVersion = "2.3.0"


libraryDependencies +=  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
libraryDependencies +=  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion
libraryDependencies +=  "org.apache.hadoop" % "hadoop-common" % hadoopVersion


libraryDependencies +=  "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
