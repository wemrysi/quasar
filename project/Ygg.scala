package quasar.project

import sbt._, Keys._
import wartremover._
import scala.Seq

object Ygg {
  def imports = """
    import quasar._, Predef._, sql._
    import scalaz._, Scalaz._
    import matryoshka._, Recursive.ops._, FunctorT.ops._, TraverseT.nonInheritedOps._
    import quasar.physical.jsonfile.fs._
  """.trim

  def testImports = imports + "\n" + """
    import quasar.sql.SqlQueries._
    import quasar.sql.fixpoint._
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

  def macros(p: Project): Project = ( p
    .dependsOn('foundation % BothScopes, 'frontend)
    .settings(name := "quasar-macros-internal")
    .settings(wartremoverWarnings in (Compile, compile) --= yggDropWarts)
  )

  def macros1(p: Project): Project = ( p
    .dependsOn('foundation % BothScopes)
    .settings(name := "quasar-macros1-internal")
    .settings(wartremoverWarnings in (Compile, compile) --= yggDropWarts)
  )

  def ygg(p: Project): Project = ( p
    .dependsOn('foundation % BothScopes, 'macros1, 'ejson)
    .settings(name := "quasar-ygg-internal")
    .settings(scalacOptions ++= Seq("-language:_"))
    .settings(libraryDependencies ++= Dependencies.ygg)
    .settings(wartremoverWarnings in (Compile, compile) --= yggDropWarts)
  )

  def jsonfile(p: Project): Project = ( p
    .dependsOn('connector % BothScopes, 'ygg % BothScopes)
    .settings(name := "quasar-jsonfile-internal")
    .settings(initialCommands in (Compile, console) := imports)
    .settings(initialCommands in (Test, console) := testImports)
  )
}
