package precogbuild

import sbt._, Keys._

object PlatformBuild {
  val BothScopes = "compile->compile;test->test"

  def envArgs     = sys.env.getOrElse("ARGS", "").trim split "\\s+" toList
  def warningOpts = Seq("-g:vars", "-deprecation", "-unchecked", "-Ywarn-unused", "-Ywarn-unused-import", "-Ywarn-numeric-widen")

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
  def kindProjector                                 = "org.spire-math" % "kind-projector" % "0.8.0" cross CrossVersion.binary

  implicit class ProjectOps(val p: sbt.Project) {
    def noArtifacts: Project = also(
                publish := (()),
           publishLocal := (()),
         Keys.`package` := file(""),
             packageBin := file(""),
      packagedArtifacts := Map()
    )
    def root: Project                                 = p in file(".")
    def also(ss: Seq[Setting[_]]): Project            = p settings (ss: _*)
    def also(s: Setting[_], ss: Setting[_]*): Project = also(s :: ss.toList)
    def deps(ms: ModuleID*): Project                  = also(libraryDependencies ++= ms.toList)
    def scalacArgs(args: String*): Project            = also(scalacOptions ++= args.toList)
    def strictVersions: Project                       = also(conflictManager := ConflictManager.strict)
    def serialTests: Project                          = also(parallelExecution in Test := false)
    def withWarnings: Project                         = scalacArgs(warningOpts: _*)
    def logImplicits: Project                         = scalacArgs("-Xlog-implicits")
    def crossJavaTargets: Project                     = also(inBoth(doubleCross))
    def scalacPlugins(ms: ModuleID*): Project         = also(ms.toList map (m => addCompilerPlugin(m)))

    // Without serialTests:
    //
    // 01:45:20.675961 [error] ! Set and retrieve an arbitrary jvalue at an arbitrary path (4 seconds, 245 ms)
    // 01:45:20.676317 [error]  java.lang.RuntimeException: Exception raised on property evaluation.
    // 01:45:20.676458 [error]  > Exception: java.lang.RuntimeException: JValue insert would overwrite existing data:
    // 01:45:20.676560 [error]    "gamux" \ [0] := [-4.049360E-1250229059, false]
    // 01:45:20.676688 [error]  Initial call was
    // 01:45:20.676780 [error]    [-4.049360E-1250229059, false] \ [0][0] := [-4.049360E-1250229059, false] (JValue.scala:101)
    //
    //               fork in Test :=  true,
    // testForkedParallel in Test :=  true,

    def setup: Project = (
      serialTests scalacPlugins (kindProjector) also (
                      organization :=  "com.precog",
                           version :=  "0.1",
                     scalacOptions ++= envArgs,
                      scalaVersion :=  "2.11.8",
               logBuffered in Test :=  false
      )
    )
  }
}
