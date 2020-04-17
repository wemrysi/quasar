import quasar.project._

import sbt.TestFrameworks.Specs2

import java.lang.{Integer, String}
import scala.{Boolean, Some, StringContext, sys}
import scala.Predef._
import scala.collection.Seq

import Versions._

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/precog/quasar"),
    "scm:git@github.com:precog/quasar.git"
  )
)

lazy val qdataVersion = Def.setting[String](
  managedVersions.value("precog-qdata"))

lazy val tectonicVersion = Def.setting[String](
  managedVersions.value("precog-tectonic"))

lazy val fs2JobVersion = Def.setting[String](
  managedVersions.value("precog-fs2-job"))

lazy val buildSettings = Seq(
  initialize := {
    val version = sys.props("java.specification.version")
    assert(
      Integer.parseInt(version.split("\\.")(1)) >= 8,
      "Java 8 or above required, found " + version)
  },

  // NB: -Xlint triggers issues that need to be fixed
  scalacOptions --= Seq("-Xlint"),

  testOptions in Test := Seq(Tests.Argument(Specs2, "exclude", "exclusive", "showtimes")),

  logBuffered in Test := githubIsWorkflowBuild.value,

  console := { (console in Test).value }, // console alias test:console

  /*
   * This plugin fixes a number of problematic cases in the for-comprehension
   * desugaring. Notably, it eliminates a non-tail-recursive case which causes
   * Slice#allFromRValues to not free memory, so it's not just a convenience or
   * an optimization.
   */
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full))

// In Travis, the processor count is reported as 32, but only ~2 cores are
// actually available to run.
concurrentRestrictions in Global := {
  val maxTasks = 4
  if (githubIsWorkflowBuild.value)
    // Recreate the default rules with the task limit hard-coded:
    Seq(Tags.limitAll(maxTasks), Tags.limit(Tags.ForkedTestGroup, 1))
  else
    (concurrentRestrictions in Global).value
}

lazy val publishSettings = Seq(
  performMavenCentralSync := false,   // publishes quasar to bintray only, skipping sonatype and maven central
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Test, packageBin) := true
)

// Build and publish a project, excluding its tests.
lazy val commonSettings =
  buildSettings ++ publishSettings ++ Seq(
    // TODO: Should be able to remove once using modern versions of pathy/matryoshka
    excludeDependencies += ExclusionRule("org.typelevel", "scala-library"))

lazy val root = project.in(file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .aggregate(
    api,
    common, connector, core,
    ejson,
    foundation, frontend,
    impl,
    qscript, qsu,
    runp,
    sql)

/** Very general utilities, ostensibly not Quasar-specific, but they just aren’t
  * in other places yet. This also contains `contrib` packages for things we’d
  * like to push to upstream libraries.
  */
lazy val foundation = project
  .settings(name := "quasar-foundation")
  .settings(commonSettings)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "quasar.build",

    libraryDependencies ++= Seq(
      "com.slamdata"               %% "slamdata-predef"           % "0.1.2",
      "org.scalaz"                 %% "scalaz-core"               % scalazVersion,
      "com.codecommit"             %% "shims"                     % "2.1.0",
      "com.codecommit"             %% "skolems"                   % skolemsVersion,
      "org.typelevel"              %% "cats-effect"               % catsEffectVersion,
      "org.typelevel"              %% "cats-effect-laws"          % catsEffectVersion % Test,
      "co.fs2"                     %% "fs2-core"                  % fs2Version,
      "co.fs2"                     %% "fs2-io"                    % fs2Version,
      "com.github.julien-truffaut" %% "monocle-core"              % monocleVersion,
      "org.typelevel"              %% "algebra"                   % algebraVersion,
      "org.typelevel"              %% "spire"                     % spireVersion,
      "io.argonaut"                %% "argonaut"                  % argonautVersion,
      "io.argonaut"                %% "argonaut-scalaz"           % argonautVersion,
      "com.slamdata"               %% "matryoshka-core"           % matryoshkaVersion,
      "com.slamdata"               %% "pathy-core"                % pathyVersion,
      "com.slamdata"               %% "pathy-argonaut"            % pathyVersion,
      "com.precog"                 %% "qdata-time"                % qdataVersion.value,
      "eu.timepit"                 %% "refined"                   % refinedVersion,
      "com.chuusai"                %% "shapeless"                 % shapelessVersion,
      "com.propensive"             %% "contextual"                % "1.2.1",
      "io.frees"                   %% "iotaz-core"                % "0.3.10",
      "com.github.markusbernhardt"  % "proxy-vole"                % "1.0.5",
      "org.typelevel"              %% "simulacrum"                % "1.0.0",
      "org.typelevel"              %% "algebra-laws"              % algebraVersion                       % Test,
      "org.typelevel"              %% "discipline"                % disciplineVersion                    % Test,
      "org.scalacheck"             %% "scalacheck"                % scalacheckVersion                    % Test,
      "org.typelevel"              %% "spire-laws"                % spireVersion                         % Test,
      "org.specs2"                 %% "specs2-core"               % specsVersion                         % Test,
      "org.specs2"                 %% "specs2-scalacheck"         % specsVersion                         % Test,
      "org.specs2"                 %% "specs2-scalaz"             % specsVersion                         % Test,
      "org.scalaz"                 %% "scalaz-scalacheck-binding" % (scalazVersion + "-scalacheck-1.14") % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.3"                              % Test))
  .enablePlugins(BuildInfoPlugin)

/** Types and interfaces describing Quasar's functionality. */
lazy val api = project
  .settings(name := "quasar-api")
  .dependsOn(foundation % BothScopes)
  .settings(
    // this does strange things with UntypedDestination.scala
    scalacOptions -= "-Ywarn-dead-code",

    libraryDependencies ++= Seq(
      "com.github.julien-truffaut" %% "monocle-macro"      % monocleVersion,
      "com.codecommit"             %% "skolems"            % skolemsVersion,
      "eu.timepit"                 %% "refined-scalaz"     % refinedVersion,
      "eu.timepit"                 %% "refined-scalacheck" % refinedVersion % Test))
  .settings(commonSettings)

/** A fixed-point implementation of the EJson spec. This should probably become
  * a standalone library.
  */
lazy val ejson = project
  .settings(name := "quasar-ejson")
  .dependsOn(foundation % BothScopes)
  .settings(
    libraryDependencies ++= Seq(
      "com.precog" %% "qdata-core" % qdataVersion.value,
      "com.precog" %% "qdata-time" % qdataVersion.value,
      "com.precog" %% "qdata-core" % qdataVersion.value % "test->test" classifier "tests",
      "com.precog" %% "qdata-time" % qdataVersion.value % "test->test" classifier "tests"))
  .settings(commonSettings)

/** Quasar components shared by both frontend and connector. This includes
  * things like data models, types, etc.
  */
lazy val common = project
  .settings(name := "quasar-common")
  .dependsOn(
    foundation % BothScopes,
    ejson)
  .settings(
    libraryDependencies ++= Seq(
      "com.precog" %% "qdata-core" % qdataVersion.value,
      "com.precog" %% "qdata-core" % qdataVersion.value % "test->test" classifier "tests",
      "com.precog" %% "qdata-time" % qdataVersion.value % "test->test" classifier "tests"))
  .settings(commonSettings)

/** Types and operations needed by query language implementations.
  */
lazy val frontend = project
  .settings(name := "quasar-frontend")
  .dependsOn(
    api,
    common % BothScopes,
    ejson % BothScopes)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.precog"                 %% "qdata-json"    % qdataVersion.value,
      "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion,
      "org.typelevel"              %% "algebra-laws"  % algebraVersion % Test))

/** Implementation of the SQL² query language.
  */
lazy val sql = project
  .settings(name := "quasar-sql")
  .dependsOn(common % BothScopes)
  .settings(commonSettings)
  .settings(

    libraryDependencies ++= Seq(
      "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion,
      "org.scala-lang.modules"     %% "scala-parser-combinators" % "1.1.2"))

lazy val qscript = project
  .settings(name := "quasar-qscript")
  .dependsOn(
    api,
    frontend % BothScopes)
  .settings(commonSettings)

lazy val qsu = project
  .settings(name := "quasar-qsu")
  .dependsOn(qscript % BothScopes)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % slf4sVersion,
      "org.typelevel" %% "cats-laws"        % catsVersion       % Test,
      "org.typelevel" %% "cats-kernel-laws" % catsVersion       % Test,
      "org.typelevel" %% "discipline"       % disciplineVersion % Test))
  .settings(scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Ypatmat-exhaust-depth", "40")
      case _ => Seq()
    }
  })

lazy val connector = project
  .settings(name := "quasar-connector")
  .dependsOn(
    foundation % "test->test",
    qscript)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.precog" %% "tectonic" % tectonicVersion.value))

lazy val core = project
  .settings(name := "quasar-core")
  .dependsOn(
    frontend % BothScopes,
    sql % BothScopes)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.github.julien-truffaut" %% "monocle-macro"             % monocleVersion,
      "com.slamdata"               %% "pathy-argonaut"            % pathyVersion))

/** Implementations of the Quasar API. */
lazy val impl = project
  .settings(name := "quasar-impl")
  .dependsOn(
    api % BothScopes,
    common % "test->test",
    connector % BothScopes,
    frontend % "test->test")
  .settings(commonSettings)
  .settings(

    libraryDependencies ++= Seq(
      "com.precog"     %% "fs2-job"                  % fs2JobVersion.value,
      "com.precog"     %% "qdata-tectonic"           % qdataVersion.value,
      "com.precog"     %% "tectonic-fs2"             % tectonicVersion.value,
      "org.http4s"     %% "jawn-fs2"                 % jawnfs2Version,
      "org.slf4s"      %% "slf4s-api"                % slf4sVersion,
      "io.argonaut"    %% "argonaut-jawn"            % argonautVersion,
      "org.typelevel"  %% "jawn-util"                % jawnVersion,
      "io.atomix"      % "atomix"                    % atomixVersion excludeAll(ExclusionRule(organization = "io.netty")),
      "org.scodec"     %% "scodec-bits"              % scodecBitsVersion,
      "io.netty"       % "netty-all"                 % nettyVersion,
      "org.mapdb"      % "mapdb"                     % mapdbVersion,
      // woodstox is added here as a quick and dirty way to get azure working
      // see ch3385 for details
      "com.fasterxml.woodstox" % "woodstox-core" % "6.0.2"))
  .evictToLocal("FS2_JOB_PATH", "core")

lazy val runp = (project in file("run"))
  .settings(name := "quasar-run")
  .dependsOn(
    core % BothScopes,
    impl,
    qsu)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.mapdb" %  "mapdb"  % mapdbVersion,
      "eu.timepit" %% "refined-scalacheck" % refinedVersion % Test))
