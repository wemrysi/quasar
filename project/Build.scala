package precog

import sbt._, Keys._

object PlatformBuild {
  val BothScopes = "compile->compile;test->test"

  def optimizeOpts = if (sys.props contains "precog.optimize") Seq("-optimize") else Seq()
  def debugOpts    = if (sys.props contains "precog.dev") Seq("-deprecation", "-unchecked") else Seq()

  /** Watch out Jonesy! It's the ol' double-cross!
   *  Why, you...
   *
   *  Given a path like src/main/scala we want that to explode into something like the
   *  following, assuming we're currently building with java 1.7 and scala 2.10.
   *
   *    src/main/scala
   *    src/main/scala_2.10
   *    src/main_1.7/scala
   *    src/main_1.7/scala_2.10
   *
   *  Similarly for main/test, 2.10/2.11, 1.7/1.8.
   */
  def doubleCross(config: Configuration) = {
    unmanagedSourceDirectories in config ++= {
      val jappend = Seq("", "_" + javaSpecVersion)
      val sappend = Seq("", "_" + scalaBinaryVersion.value)
      val basis   = (sourceDirectory in config).value
      val parent  = basis.getParentFile
      val name    = basis.getName
      for (j <- jappend ; s <- sappend) yield parent / s"$name$j" / s"scala$s"
    }
  }

  def javaSpecVersion: String                       = sys.props("java.specification.version")
  def inBoth[A](f: Configuration => Seq[A]): Seq[A] = List(Test, Compile) flatMap f

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
    def compileArgs(args: String*): Project           = also(scalacOptions in Compile ++= args.toList)

    def setup: Project = (
      also(
                   organization :=  "com.precog",
                        version :=  "2.6.1-SNAPSHOT",
                  scalacOptions ++= Seq("-g:vars") ++ optimizeOpts ++ debugOpts,
                   scalaVersion :=  "2.11.8",
             crossScalaVersions :=  Seq("2.9.3", "2.10.6", "2.11.8"),
            logBuffered in Test :=  false,
                       ivyScala :=  ivyScala.value map (_.copy(overrideScalaVersion = true))
      )
      also inBoth(doubleCross)
      also addCompilerPlugin("org.spire-math" % "kind-projector" % "0.8.0" cross CrossVersion.binary)
      // compileArgs("-Ywarn-unused", "-Ywarn-unused-import")
      // "-Ywarn-numeric-widen", "-Xlog-implicits"
    )
  }
}
