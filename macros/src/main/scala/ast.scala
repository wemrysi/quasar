package ygg.json

package object ast {
  type ->[+A, +B] = (A, B)
  type Vec[+A]    = scala.Vector[A]
  val Vec         = scala.Vector
}
