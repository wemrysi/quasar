import sbt._
import Keys._

object BlueEyesBuild extends Build {
  val nexusSettings : Seq[Project.Setting[_]] = Seq(
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { (repo: MavenRepository) => false },

    pomExtra :=
      <url>https://github.com/jdegoes/blueeyes</url>
      <licenses>
        <license>
          <name>MIT license</name>
          <url>http://www.opensource.org/licenses/mit-license.php</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <connection>scm:git:git@github.com/jdegoes/blueeyes.git</connection>
        <developerConnection>scm:git:git@github.com/jdegoes/blueeyes.git</developerConnection>
        <url>https://github.com/jdegoes/blueeyes</url>
      </scm>
      <developers>
        <developer>
          <id>jdegoes</id>
          <name>John De Goes</name>
        </developer>
        <developer>
          <id>nuttycom</id>
          <name>Kris Nuttycombe</name>
        </developer>
        <developer>
          <id>mlagutko</id>
          <name>Michael Lagutko</name>
        </developer>
        <developer>
          <id>dchenbecker</id>
          <name>Derek Chen-Becker</name>
        </developer>
      </developers>
  )

  val scalazVersion = "7.0.9"

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

  lazy val blueeyes = Project(id = "blueeyes", base = file(".")).settings((nexusSettings ++ commonSettings): _*) aggregate(util, json, akka_testing, bkka, core, test, mongo)

  lazy val util  = Project(id = "util", base = file("util")).settings((nexusSettings ++ commonSettings): _*)

  lazy val json  = Project(id = "json", base = file("json")).settings((nexusSettings ++ commonSettings): _*)

  lazy val akka_testing  = Project(id = "akka_testing", base = file("akka_testing")).settings((nexusSettings ++ commonSettings): _*) dependsOn util

  lazy val bkka  = Project(id = "bkka", base = file("bkka")).settings((nexusSettings ++ commonSettings): _*) dependsOn util

  lazy val core  = Project(id = "core", base = file("core")).settings((nexusSettings ++ commonSettings): _*) dependsOn (util, json, bkka, akka_testing)

  lazy val test  = Project(id = "test", base = file("test")).settings((nexusSettings ++ commonSettings): _*) dependsOn core

  lazy val mongo = Project(id = "mongo", base = file("mongo")).settings((nexusSettings ++ commonSettings): _*) dependsOn (core, json % "test->test")
}
