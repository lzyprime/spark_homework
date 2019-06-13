name := "HomeWork"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.2.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.0",
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.hbase" % "hbase-client" % "1.3.1",
  "org.apache.hbase" % "hbase-common" % "1.3.1",
  "org.apache.hbase" % "hbase-server" % "1.3.1",
)

