package blueeyes

import sbt._
import Keys._

object BlueeyesBuild {
  // <dependency org="org.eclipse.jetty" name="jetty-server" rev="8.1.3.v20120416" conf="test">
  //   <exclude org="org.eclipse.jetty.orbit" />
  // </dependency>

  implicit class ProjectOps(val p: sbt.Project) {
    def noArtifacts: Project = also(
                publish := (()),
           publishLocal := (()),
         Keys.`package` := file(""),
             packageBin := file(""),
      packagedArtifacts := Map()
    )
    def root: Project                                 = p in file(".")
    def inBothScopes: ClasspathDependency             = p % "compile->compile;test->test"
    def inTestScope: ClasspathDependency              = p % "test->test"
    def also(ss: Seq[Setting[_]]): Project            = p settings (ss: _*)
    def also(s: Setting[_], ss: Setting[_]*): Project = also(s +: ss.toSeq)
    def deps(ms: ModuleID*): Project                  = also(libraryDependencies ++= ms.toSeq)

    def setup: Project = also(
                              name :=  ("blueeyes-" + p.id),
                      scalaVersion :=  "2.9.3",
                      organization :=  "com.reportgrid",
                           version :=  "1.0.0-M9.5",
               logBuffered in Test :=  false,
         parallelExecution in Test :=  false,
            libraryDependencies ++= Seq(
      //   "org.scalaz"     %% "scalaz-effect"   %  "7.0.9",
      //   "org.specs2"     %% "specs2"          % "1.12.4.1" % Test,
        "org.scalacheck" %% "scalacheck"      %  "1.10.1"  % Test
      //   "ch.qos.logback"  % "logback-classic" %  "1.0.0"   % Test
      ),
          ivyScala :=  ivyScala.value map (_.copy(overrideScalaVersion = true))
    )
  }
}
