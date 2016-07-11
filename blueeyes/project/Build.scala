package blueeyes

import sbt._
import Keys._

object BlueeyesBuild {
  val BothScopes = "compile->compile;test->test"

  implicit class ProjectOps(val p: sbt.Project) {
    def noArtifacts: Project = also(
                publish := (()),
           publishLocal := (()),
         Keys.`package` := file(""),
             packageBin := file(""),
      packagedArtifacts := Map()
    )
    def root: Project                                 = p in file(".")
    def inTestScope: ClasspathDependency              = p % "test->test"
    def also(ss: Seq[Setting[_]]): Project            = p settings (ss: _*)
    def also(s: Setting[_], ss: Setting[_]*): Project = also(s +: ss.toSeq)
    def deps(ms: ModuleID*): Project                  = also(libraryDependencies ++= ms.toSeq)

    def setup: Project = also(
                            name := s"blueeyes-${p.id}",
                    scalaVersion := "2.9.3",
                    organization := "com.reportgrid",
                         version := "1.0.0-M9.5",
             logBuffered in Test := false,
       parallelExecution in Test := false,
             libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % Test,
                        ivyScala := ivyScala.value map (_.copy(overrideScalaVersion = true))
    )
  }
}
