package quasar
package macros

import spire.macros.Syntax

object Spire {
  def cfor[A](init:A)(test:A => Boolean, next:A => A)(body:A => Unit): Unit = macro Syntax.cforMacro[A]
  def cforRange(r: Range)(body: Int => Unit): Unit                          = macro Syntax.cforRangeMacro
  def cforRange2(r1: Range, r2: Range)(body: (Int, Int) => Unit): Unit      = macro Syntax.cforRange2Macro
}
