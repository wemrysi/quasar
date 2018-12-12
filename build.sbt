import quasar.project._

import sbt.TestFrameworks.Specs2

import java.lang.{Integer, String}
import scala.{Boolean, Some, StringContext, sys}
import scala.Predef._
import scala.collection.Seq

lazy val buildSettings = Seq(
  initialize := {
    val version = sys.props("java.specification.version")
    assert(
      Integer.parseInt(version.split("\\.")(1)) >= 8,
      "Java 8 or above required, found " + version)
  },

  // NB: -Xlint triggers issues that need to be fixed
  scalacOptions --= Seq("-Xlint"),

  // NB: Some warts are disabled in specific projects. Here’s why:
  //   • AsInstanceOf   – wartremover/wartremover#266
  //   • others         – simply need to be reviewed & fixed
  wartremoverWarnings in (Compile, compile) --= Seq(
    Wart.Any,                   // - see wartremover/wartremover#263
    Wart.PublicInference,       // - creates many compile errors when enabled - needs to be enabled incrementally
    Wart.ImplicitParameter,     // - creates many compile errors when enabled - needs to be enabled incrementally
    Wart.ImplicitConversion,    // - see mpilquist/simulacrum#35
    Wart.Nothing,               // - see wartremover/wartremover#263
    Wart.NonUnitStatements),    // better-monadic-for causes some spurious warnings from this

  testOptions in Test := Seq(Tests.Argument(Specs2, "exclude", "exclusive", "showtimes")),

  logBuffered in Test := isTravisBuild.value,

  console := { (console in Test).value }, // console alias test:console

  /*
   * This plugin fixes a number of problematic cases in the for-comprehension
   * desugaring. Notably, it eliminates a non-tail-recursive case which causes
   * Slice#allFromRValues to not free memory, so it's not just a convenience or
   * an optimization.
   */
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.2.4"))

// In Travis, the processor count is reported as 32, but only ~2 cores are
// actually available to run.
concurrentRestrictions in Global := {
  val maxTasks = 4
  if (isTravisBuild.value)
    // Recreate the default rules with the task limit hard-coded:
    Seq(Tags.limitAll(maxTasks), Tags.limit(Tags.ForkedTestGroup, 1))
  else
    (concurrentRestrictions in Global).value
}

lazy val publishSettings = Seq(
  performMavenCentralSync := false,   // publishes quasar to bintray only, skipping sonatype and maven central
  homepage := Some(url("https://github.com/slamdata/quasar")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/slamdata/quasar"),
      "scm:git@github.com:slamdata/quasar.git"
    )
  ),
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Test, packageBin) := true
)

// Build and publish a project, excluding its tests.
lazy val commonSettings = buildSettings ++ publishSettings

lazy val isCIBuild = settingKey[Boolean]("True when building in any automated environment (e.g. Travis)")
lazy val isIsolatedEnv = settingKey[Boolean]("True if running in an isolated environment")

lazy val root = project.in(file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .aggregate(
    api,
    blueeyes,
    common, connector, core,
    datagen,
    ejson,
    foundation, frontend,
    impl, it,
    mimir,
    niflheim,
    qscript, qsu,
    runp,
    sql, sst,
    yggdrasil, yggdrasilPerf
  ).enablePlugins(AutomateHeaderPlugin)

/** Very general utilities, ostensibly not Quasar-specific, but they just aren’t
  * in other places yet. This also contains `contrib` packages for things we’d
  * like to push to upstream libraries.
  */
lazy val foundation = project
  .settings(name := "quasar-foundation-internal")
  .settings(commonSettings)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](version, isCIBuild, isIsolatedEnv),
    buildInfoPackage := "quasar.build",
    isCIBuild := isTravisBuild.value,
    isIsolatedEnv := java.lang.Boolean.parseBoolean(java.lang.System.getProperty("isIsolatedEnv")),
    libraryDependencies ++= Dependencies.foundation)
  .enablePlugins(AutomateHeaderPlugin, BuildInfoPlugin)

/** Types and interfaces describing Quasar's functionality. */
lazy val api = project
  .settings(name := "quasar-api-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.api)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** A fixed-point implementation of the EJson spec. This should probably become
  * a standalone library.
  */
lazy val ejson = project
  .settings(name := "quasar-ejson-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.ejson)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** Quasar components shared by both frontend and connector. This includes
  * things like data models, types, etc.
  */
lazy val common = project
  .settings(name := "quasar-common-internal")
  .dependsOn(
    foundation % BothScopes,
    ejson)
  .settings(libraryDependencies ++= Dependencies.common)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** Types and operations needed by query language implementations.
  */
lazy val frontend = project
  .settings(name := "quasar-frontend-internal")
  .dependsOn(
    common % BothScopes,
    ejson % BothScopes)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Dependencies.frontend)
  .enablePlugins(AutomateHeaderPlugin)

lazy val sst = project
  .settings(name := "quasar-sst-internal")
  .dependsOn(frontend % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val datagen = project
  .settings(name := "quasar-datagen")
  .dependsOn(sst % BothScopes)
  .settings(commonSettings)
  .settings(
    mainClass in Compile := Some("quasar.datagen.Main"),
    libraryDependencies ++= Dependencies.datagen)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the SQL² query language.
  */
lazy val sql = project
  .settings(name := "quasar-sql-internal")
  .dependsOn(common % BothScopes)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Dependencies.sql)
  .enablePlugins(AutomateHeaderPlugin)

lazy val qscript = project
  .settings(name := "quasar-qscript-internal")
  .dependsOn(
    frontend % BothScopes,
    api)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val qsu = project
  .settings(name := "quasar-qsu-internal")
  .dependsOn(qscript % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.qsu)
  .settings(scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Ypatmat-exhaust-depth", "40")
      case _ => Seq()
    }
  })
  .enablePlugins(AutomateHeaderPlugin)

lazy val connector = project
  .settings(name := "quasar-connector-internal")
  .dependsOn(
    foundation % "test->test",
    qscript)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .settings(name := "quasar-core-internal")
  .dependsOn(
    frontend % BothScopes,
    sql % BothScopes)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Dependencies.core)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementations of the Quasar API. */
lazy val impl = project
  .settings(name := "quasar-impl-internal")
  .dependsOn(
    api % BothScopes,
    common % "test->test",
    connector % BothScopes,
    frontend % "test->test",
    sst)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.impl)
  .enablePlugins(AutomateHeaderPlugin)

lazy val runp = (project in file("run"))
  .settings(name := "quasar-run")
  .dependsOn(
    core % BothScopes,
    impl,
    qsu)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** Integration tests that have some dependency on a running connector.
  */
lazy val it = project
  .settings(name := "quasar-it-internal")
  .dependsOn(
    qscript % "test->test",
    mimir)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.it)
  .settings(parallelExecution in Test := false)
  .settings(logBuffered in Test := false)
  .enablePlugins(AutomateHeaderPlugin)

lazy val blueeyes = project
  .settings(
    name := "quasar-blueeyes-internal",
    scalacStrictMode := false,
    scalacOptions += "-language:postfixOps")
  .dependsOn(frontend % BothScopes)
  .settings(libraryDependencies ++= Dependencies.blueeyes)
  .settings(logBuffered in Test := isTravisBuild.value)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val niflheim = project
  .settings(
    name := "quasar-niflheim-internal",
    scalacStrictMode := false)
  .dependsOn(blueeyes % BothScopes)
  .settings(libraryDependencies ++= Dependencies.niflheim)
  .settings(logBuffered in Test := isTravisBuild.value)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val yggdrasil = project
  .settings(
    name := "quasar-yggdrasil-internal",
    scalacStrictMode := false,
    scalacOptions += "-language:postfixOps")
  .dependsOn(niflheim % BothScopes)
  .settings(
    resolvers += "bintray-djspiewak-maven" at "https://dl.bintray.com/djspiewak/maven",
    libraryDependencies ++= Dependencies.yggdrasil)
  .settings(logBuffered in Test := isTravisBuild.value)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val yggdrasilPerf = project
  .settings(
    name := "quasar-yggdrasil-perf-internal",
    scalacStrictMode := false,
    javaOptions += "-XX:+HeapDumpOnOutOfMemoryError")
  .dependsOn(yggdrasil % "compile->compile;compile->test")
  .settings(logBuffered in Test := isTravisBuild.value)
  .settings(headerLicenseSettings)
  .settings(noPublishSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(JmhPlugin)

lazy val mimir = project
  .settings(
    name := "quasar-mimir-internal",
    scalacStrictMode := false,
    scalacOptions += "-language:postfixOps")
  .dependsOn(
    yggdrasil % BothScopes,
    impl % BothScopes,
    runp)
  .settings(logBuffered in Test := isTravisBuild.value)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .enablePlugins(AutomateHeaderPlugin)
