package quasar.project

import sbt._, Keys._
import wartremover._
import scala.Seq

object Ygg {
  def imports = """
    import quasar._, Predef._
    import scalaz._, Scalaz._
    import matryoshka._, Recursive.ops._, FunctorT.ops._, TraverseT.nonInheritedOps._
  """.trim

  def yggImports = imports + "\n" + """
    import ygg._, common._, json._, table._, trans._
    import quasar._, sql._, SemanticAnalysis._
    import ygg.table.{ PlayTable => p }

    def zips = PlayTable(new jFile("it/src/main/resources/tests/zips.data"))
    def lp(q: String): Fix[LogicalPlan] = (
      compile(q) map Optimizer.optimize flatMap (q =>
        (LogicalPlan ensureCorrectTypes q).disjunction
          leftMap (_.list.toList mkString ";")
      )
    ).fold(abort, x => x)
    def zq = lp("select * from zips where state=\"CO\" limit 3")

  """.trim

  def jsonfileTestImports = imports + "\n" + """
    import quasar.physical.jsonfile.fs._
    import quasar.sql._, SqlQueries._, fixpoint._
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
    .settings(scalacOptions += "-language:experimental.macros")
  )

  def macros1(p: Project): Project = ( p
    .dependsOn('foundation % BothScopes)
    .settings(name := "quasar-macros1-internal")
    .settings(wartremoverWarnings in (Compile, compile) --= yggDropWarts)
    .settings(scalacOptions += "-language:experimental.macros")
  )

  def ygg(p: Project): Project = ( p
    .dependsOn('foundation % BothScopes, 'macros1, 'ejson, 'connector)
    .settings(name := "quasar-ygg-internal")
    .settings(scalacOptions ++= Seq("-language:_"))
    .settings(libraryDependencies ++= Dependencies.ygg)
    .settings(wartremoverWarnings in (Compile, compile) --= yggDropWarts)
    .settings(initialCommands in (Compile, console) := yggImports)
  )

  def jsonfile(p: Project): Project = ( p
    .dependsOn('connector % BothScopes, 'ygg % BothScopes)
    .settings(name := "quasar-jsonfile-internal")
    .settings(initialCommands in (Compile, console) := imports)
    .settings(initialCommands in (Test, console) := jsonfileTestImports)
  )
}
