import sbt._
import Keys._

object BlueEyesBuild extends Build {
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

  lazy val akka_testing = Project(id = "akka_testing", base = file("akka_testing")).settings(commonSettings: _*) dependsOn util
  lazy val bkka         = Project(id = "bkka", base = file("bkka")).settings(commonSettings: _*) dependsOn util
  lazy val blueeyes     = Project(id = "blueeyes", base = file(".")).settings(commonSettings: _*) aggregate(util, json, akka_testing, bkka, core, test)
  lazy val core         = Project(id = "core", base = file("core")).settings(commonSettings: _*) dependsOn (util, json, bkka, akka_testing)
  lazy val json         = Project(id = "json", base = file("json")).settings(commonSettings: _*)
  lazy val test         = Project(id = "test", base = file("test")).settings(commonSettings: _*) dependsOn core
  lazy val util         = Project(id = "util", base = file("util")).settings(commonSettings: _*)
}
