/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */

package precog

import sbt._, Keys._

object PlatformBuild {
  val profileTask     = InputKey[Unit]("profile", "Runs the given project under JProfiler")
  val blueeyesVersion = "1.0.0-M9.5"

  def optimizeOpts   = if (sys.props contains "com.precog.build.optimize") Seq("-optimize") else Seq()
  def commonSettings = Seq(
                               organization :=  "com.precog",
                                    version :=  "2.6.1-SNAPSHOT",
                              scalacOptions ++= Seq("-g:none"), // "-deprecation", "-unchecked"
                              scalacOptions ++= optimizeOpts,
                               javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
                               scalaVersion :=  "2.9.3",
    (unmanagedSourceDirectories in Compile) <<= (scalaSource in Compile, javaSource in Compile)(Seq(_) ++ Set(_)),
       (unmanagedSourceDirectories in Test) <<= (scalaSource in Test)(Seq(_)),
                  parallelExecution in Test :=  false,
                        logBuffered in Test :=  false,
              resolvers +=  "Akka Repo" at "http://repo.akka.io/repository",
    libraryDependencies ++= Seq(
      "org.slf4s"                %% "slf4s-api"   % "1.7.13",
      "org.scalaz"               %% "scalaz-core" % "7.0.9",
      "org.scalacheck"           %% "scalacheck"  % "1.10.1"  % "test",
      "com.google.code.findbugs"  % "jsr305"      %  "3.0.1"
    )
  )

  def jprofilerArg(base: File) = "-agentpath:%s=offline,config=%s/%s,id=%s".format(
    "/Applications/jprofiler7/bin/macos/libjprofilerti.jnilib",
    base,
    "src/main/resources/jprofile.xml",
    "116"
  )

  def jprofilerSettings = Seq(
                   fork in run :=  true,
           fork in profileTask :=  true,
    javaOptions in profileTask <<= (javaOptions, baseDirectory) map ((opts, b) => opts :+ jprofilerArg(b))
  )

  implicit class ProjectOps(val p: sbt.Project) {
    import sbtassembly.AssemblyPlugin.assemblySettings
    import sbtassembly.AssemblyPlugin.autoImport._

    // https://github.com/sbt/sbt-assembly
    //
    // By the way, the first case pattern in the above using PathList(...) is how
    // you can pick javax/servlet/* from the first jar. If the default
    // MergeStrategy.deduplicate is not working for you, that likely means you have
    // multiple versions of some library pulled by your dependency graph. The real
    // solution is to fix that dependency graph. You can work around it by
    // MergeStrategy.first but don't be surprised when you see
    // ClassNotFoundException.
    private def assemblyMerger(path: String) = path match {
      case s if s endsWith ".txt"                    => MergeStrategy.discard
      case PathList("META-INF", "MANIFEST.MF")       => MergeStrategy.discard
      case PathList("org", "slf4j", "impl", xs @ _*) => MergeStrategy.first
      case _                                         => MergeStrategy.deduplicate
    }

    def also(ss: Seq[Setting[_]]): Project            = p settings (ss: _*)
    def also(s: Setting[_], ss: Setting[_]*): Project = also(s +: ss.toSeq)
    def deps(ms: ModuleID*): Project                  = also(libraryDependencies ++= ms.toSeq)
    def root: Project                                 = p in file(".")
    def testLogging: Project                          = p dependsOn LocalProject("logging") % "test->test"
    def usesCommon: Project                           = p dependsOn LocalProject("common") % "compile->compile;test->test"
    def setup: Project                                = p dependsOn LocalProject("blueeyes") also commonSettings
    def assemblyProject: Project                      = p.setup.testLogging also assemblySettings also (
                       test in assembly :=  (),
            assemblyJarName in assembly :=  "%s-assembly-%s.jar".format(name.value, "git describe".!!.trim),
                     target in assembly <<= target,
      assemblyMergeStrategy in assembly :=  assemblyMerger
    )
  }
}
