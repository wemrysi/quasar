package quasar.project

import scala.{Option, Boolean}
import java.lang.{String, System}
import scala.collection.Seq

import sbt._, Keys._
import slamdata.CommonDependencies

object Dependencies {
  private val disciplineVersion = "0.5"
  private val jawnVersion       = "0.8.4"
  private val jacksonVersion    = "2.4.4"
  private val matryoshkaVersion = "0.16.4"
  private val pathyVersion      = "0.2.9"
  private val raptureVersion    = "2.0.0-M6"
  private val scodecBitsVersion = "1.1.0"
  // For unknown reason sbt-slamdata's specsVersion, 3.8.7,
  // leads to a ParquetRDDE failure under a full test run
  private val specsVersion      = "3.8.4"

  def foundation = Seq(
    CommonDependencies.scalaz.core,
    CommonDependencies.scalaz.concurrent,
    CommonDependencies.scalaz.iteratee,
    CommonDependencies.scalazStream.scalazStream,
    CommonDependencies.monocle.core,
    CommonDependencies.argonaut.argonaut,
    CommonDependencies.argonaut.scalaz,
    "com.slamdata"  %% "matryoshka-core" % matryoshkaVersion,
    "com.slamdata"  %% "pathy-core"      % pathyVersion,
    "com.slamdata"  %% "pathy-argonaut"  % pathyVersion,
    CommonDependencies.refined.refined,
    CommonDependencies.shapeless.shapeless,
    CommonDependencies.scalacheck.scalacheck                  % Test,
    CommonDependencies.simulacrum.simulacrum                  % Test,
    "org.typelevel" %% "discipline"      % disciplineVersion  % Test,
    "org.specs2"    %% "specs2-core"     % specsVersion       % Test,
    CommonDependencies.scalaz.scalacheckBinding               % Test,
    CommonDependencies.typelevel.shapelessScalacheck          % Test,
    CommonDependencies.typelevel.scalazSpecs2                 % Test
  )

  def frontend = Seq(
    CommonDependencies.monocle.`macro`,
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.5"
  )

  def ejson = Seq(
    CommonDependencies.argonaut.argonaut,
    "org.spire-math" %% "jawn-parser" % jawnVersion
  )
  def effect = Seq(
    "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.4"
  )
  def core = Seq(
    "com.github.tototoshi" %% "scala-csv"      % "1.3.4",
    CommonDependencies.monocle.`macro`,
    CommonDependencies.http4s.core,
    "com.slamdata"         %% "pathy-argonaut" % pathyVersion
  )
  def interface = Seq(
    "com.github.scopt" %% "scopt" % "3.5.0",
    "org.jboss.aesh"    % "aesh"  % "0.66.8"
  )

  def mongodb = {
    val nettyVersion = "4.0.42.Final" // This version is set to be the same as Spark
                                      // to avoid problems in web and it where their classpaths get merged
                                      // In any case, it should be binary compatible with version 4.0.26 that this
                                      // mongo release is expecting
    Seq(
      "org.mongodb" % "mongodb-driver-async" %   "3.3.0", // Intentionnally not upgrading to the latest 3.4.1 in order
                                                          // to make integration easier with Spark as the latest version
                                                          // depends on netty 4.1.x
      // These are optional dependencies of the mongo asynchronous driver.
      // They are needed to connect to mongodb vis SSL which we do under certain configurations
      "io.netty"    % "netty-buffer"         % nettyVersion,
      "io.netty"    % "netty-handler"        % nettyVersion
    )
  }

  val postgresql = Seq(
    CommonDependencies.doobie.core     % "compile, test",
    CommonDependencies.doobie.postgres % "compile, test"
  )

  def sparkcore(sparkProvided: Boolean) = Seq(
    ("org.apache.spark" %% "spark-core" % "2.1.0" % (if(sparkProvided) "provided" else "compile"))
      .exclude("aopalliance", "aopalliance")                  // It seems crazy that we need to do this,
      .exclude("javax.inject", "javax.inject")                // but it looks like Spark had some dependency conflicts
      .exclude("commons-collections", "commons-collections")  // among its transitive dependencies which means
      .exclude("commons-beanutils", "commons-beanutils-core") // we need to exclude this stuff so that we can
      .exclude("commons-logging", "commons-logging")          // create an assembly jar without conflicts
      .exclude("commons-logging", "commons-logging")          // It would seem though that things work without them...
      .exclude("com.esotericsoftware.minlog", "minlog")       // It's likely this list will need to be updated
      .exclude("org.spark-project.spark", "unused")           // anytime the Spark dependency itselft is updated
      .exclude("org.scalatest", "scalatest_2.11"),
    "org.apache.parquet" % "parquet-format" % "2.3.1",
    "org.apache.parquet" % "parquet-hadoop" % "1.9.0",
    CommonDependencies.http4s.core
  )

  def marklogicValidation = Seq(
    CommonDependencies.refined.refined,
    CommonDependencies.scalaz.core
  )
  def marklogic = Seq(
    "com.fasterxml.jackson.core" %  "jackson-core"        % jacksonVersion,
    "com.fasterxml.jackson.core" %  "jackson-databind"    % jacksonVersion,
    "com.marklogic"              %  "marklogic-xcc"       % "8.0.5",
    CommonDependencies.refined.scalacheck                                   % Test,
    "org.scala-lang.modules"     %% "scala-xml"           % "1.0.5"
  )
  val couchbase = Seq(
    "com.couchbase.client" %  "java-client" % "2.3.5",
    "io.reactivex"         %% "rxscala"     % "0.26.3",
    CommonDependencies.http4s.core
  )
  def web = Seq(
    "org.scodec"     %% "scodec-scalaz"       % "1.3.0a",
    "org.scodec"     %% "scodec-bits"         % scodecBitsVersion,
    CommonDependencies.http4s.dsl,
    CommonDependencies.http4s.argonaut62,
    CommonDependencies.http4s.blazeClient,
    CommonDependencies.http4s.blazeServer,
    "com.propensive" %% "rapture-json"        % raptureVersion     % Test,
    "com.propensive" %% "rapture-json-json4s" % raptureVersion     % Test,
    CommonDependencies.refined.scalacheck                          % Test
  )
  def it = Seq(
    CommonDependencies.argonaut.monocle                         % Test,
    CommonDependencies.http4s.blazeClient                       % Test,
    CommonDependencies.refined.scalacheck                       % Test,
    "io.verizon.knobs" %% "core"                % "3.12.27a"    % Test)
}
