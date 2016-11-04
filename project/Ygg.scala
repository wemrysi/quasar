package quasar.project

import sbt._, Keys._
import wartremover._
import scala.{ Seq, StringContext }

object Ygg {
  def yggSetup(p: Project): Project = ( p
    .settings(name := s"quasar-${p.id}-internal")
    .settings(scalacOptions ++= Seq("-language:_"))
    .settings(scalacOptions -= "-Xfatal-warnings")
    .settings(wartremoverWarnings in (Compile, compile) --= yggDropWarts)
    .settings(scalacOptions in (Compile, console) --= Seq("-Ywarn-unused-code"))
  )

  def yggDeps = Seq(
    "org.mapdb"      %  "mapdb"        % "3.0.1",
    "org.spire-math" %% "spire-macros" % "0.12.0"
  )
  def macroDeps = Seq(
    "org.spire-math" %% "jawn-ast"      % "0.10.1",
    "org.spire-math" %% "jawn-parser"   % "0.10.1",
    "io.argonaut"    %% "argonaut-jawn" % "6.2-M3"
  )

  def imports = """
    import quasar._, Predef._
    import java.nio.file._
    import java.time._
    import scalaz._, Scalaz._
    import matryoshka._, Recursive.ops._, FunctorT.ops._, TraverseT.nonInheritedOps._
  """.trim

  def yggImports = imports + "\n" + """
    import ygg._, ygg.common._, json._, table._, trans._
    import quasar._, sql._, SemanticAnalysis._
    import ygg.repl._
  """.trim

  def jsonfileImports = yggImports + "\n" + """
    import quasar.physical.jsonfile.fs._, FallbackJV._
    val mf = quasar.qscript.MapFuncs
  """.trim

  def yggDropWarts = Seq(
    Wart.Equals,
    Wart.AsInstanceOf,
    Wart.Overloading,
    Wart.ToString,
    Wart.NoNeedForMonad,
    Wart.Null,
    Wart.Var,
    Wart.MutableDataStructures,
    Wart.NonUnitStatements,
    Wart.Return,
    Wart.While,
    Wart.ListOps,
    Wart.Throw,
    Wart.OptionPartial,
    Wart.Option2Iterable
  )

  def macros(p: Project): Project = (
     yggSetup(p)
    .dependsOn('foundation % BothScopes, 'frontend)
    .settings(libraryDependencies ++= macroDeps)
  )

  def ygg(p: Project): Project = (
     yggSetup(p)
    .dependsOn('foundation % BothScopes, 'macros, 'ejson, 'connector, 'sql)
    .settings(libraryDependencies ++= yggDeps)
    .settings(initialCommands in console := yggImports)
  )

  def jsonfile(p: Project): Project = (
     yggSetup(p)
    .dependsOn('connector, 'ygg)
    .settings(initialCommands in console := jsonfileImports)
  )
}
