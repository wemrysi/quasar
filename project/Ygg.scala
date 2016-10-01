package quasar.project

import sbt._, Keys._
import wartremover._
import scala.Seq

object Ygg {
  def yggDropWarts = Seq(
    Wart.AsInstanceOf,
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
}
