name := "SBTSpark"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

// https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client
libraryDependencies += "org.mariadb.jdbc" % "mariadb-java-client" % "2.4.1"
