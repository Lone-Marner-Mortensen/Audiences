name := "Audience"

version := "1.0"

//scalaVersion := "2.12.6"
//scalaVersion := "2.11.8"
scalaVersion := "2.12.6"

scalacOptions ++= Seq("-deprecation")

libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies += "org.mockito" % "mockito-core" % "2.23.0" % Test
//libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.1"
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
// https://mvnrepository.com/artifact/org.typelevel/cats-core
libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.1"

scalacOptions += "-feature"

scalacOptions in Test ++= Seq("-Yrangepos")




    