package quasar

import sbt._

package object project {
  val BothScopes    = "test->test;compile->compile"
  val CompileOnTest = "compile->test"

  implicit class AnyOps[A](x: A) {
    def |>[B](f: A => B): B = f(x)
  }
  implicit def symbolToProject(x: scala.Symbol) = LocalProject(x.name)
}
