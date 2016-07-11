package precog

import sbt._, Keys._

object PlatformBuild {
  val BothScopes = "compile->compile;test->test"

  def excludeBlacklist(m: ModuleID): ModuleID = ( m
    exclude("commons-codec", "commons-codec")
    exclude("javolution", "javolution")
    exclude("io.netty", "netty")
    exclude("com.googlecode.concurrentlinkedhashmap", "concurrentlinkedhashmap-lru")
    exclude("junit", "junit")
    exclude("org.scalatest", "scalatest_2.9.2")
  )

  def optimizeOpts = if (sys.props contains "precog.optimize") Seq("-optimize") else Seq()
  def debugOpts    = if (sys.props contains "precog.dev") Seq("-deprecation", "-unchecked") else Seq()

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
                   organization :=  "com.precog",
                        version :=  "2.6.1-SNAPSHOT",
                  scalacOptions ++= Seq("-g:none") ++ optimizeOpts ++ debugOpts,
                   javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
                   scalaVersion :=  "2.9.3",
      parallelExecution in Test :=  false,
            logBuffered in Test :=  false,
                       ivyScala :=  ivyScala.value map (_.copy(overrideScalaVersion = true))

    )
  }
}
