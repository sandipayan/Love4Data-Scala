import sbt._ 

organization := "com.despegar"

name := "point-in-polygon"

scalaVersion := "2.11.4"

scalacOptions ++= Seq(
        "-feature",
        "-language:postfixOps",
        "-deprecation"
)

libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "2.0.2" ,
        "org.apache.spark" %% "spark-sql" % "2.0.2" ,
        "org.scalatest"  %% "scalatest"  % "2.2.1"       % "test"
)
