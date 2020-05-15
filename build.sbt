name := "Audience"

version := "1.0"

scalaVersion := "2.12.6"

scalacOptions ++= Seq("-deprecation")

libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies += "org.mockito" % "mockito-core" % "2.23.0" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

scalacOptions += "-feature"

scalacOptions in Test ++= Seq("-Yrangepos")




    