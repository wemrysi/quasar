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

  def yggImports = """
    import java.nio.file._
    import java.time._
    import quasar._, sql._
    import quasar.qscript.{ MapFuncs => mf }
    import scalaz._, Scalaz._
    import ygg.common.{ quasarExtensionOps => _, _ }
    import ygg._, json._, table._, trans._, repl._
    import TableData.{ fromJValues => fromJson }
  """.trim

  def jsonfileImports = yggImports + "\n" + """
    import quasar.physical.jsonfile.fs._, FallbackJV._
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
    .dependsOn('foundation % BothScopes, 'macros, 'ejson, 'connector, 'sql % CompileOnTest)
    .settings(libraryDependencies ++= yggDeps)
    .settings(initialCommands in console := yggImports)
  )

  def jsonfile(p: Project): Project = (
     yggSetup(p)
    .dependsOn('connector, 'ygg)
    .settings(initialCommands in console := jsonfileImports)
  )
}
