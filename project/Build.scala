package blueeyes

import sbt._
import Keys._

object Blueeyes {
  val scalazVersion  = "7.0.9"
  val commonSettings = Seq(
           scalaVersion :=  "2.9.3",
           organization :=  "com.reportgrid",
                version :=  "1.0.0-M9.5",
    libraryDependencies ++= Seq(
      "org.scalaz"     %% "scalaz-effect"   % scalazVersion,
      "org.specs2"     %% "specs2"          %   "1.12.4.1"   % Test,
      "org.scalacheck" %% "scalacheck"      %    "1.10.1"    % Test,
      "ch.qos.logback"  % "logback-classic" %    "1.0.0"     % Test
    )
  )
}
