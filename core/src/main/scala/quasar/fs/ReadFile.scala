package quasar
package fs

import Predef._

import scalaz._
import pathy.Path._

sealed trait ReadFile[A]

object ReadFile {
  final case class ReadHandle(run: Long) extends AnyVal

  final case class Open(path: RelFile[Sandboxed], offset: Natural, limit: Option[Positive]) extends ReadFile[PathError2 \/ ReadHandle]
  final case class Read(h: ReadHandle) extends ReadFile[Vector[Data]]
  final case class Close(h: ReadHandle) extends ReadFile[Unit]
}

final class ReadFileOps[S[_]](implicit S: ReadFileF :<: S) {
  type M[A] = Free[S, A]

  def scan(path: RelFile[Sandboxed], offset: Natural, limit: Option[Positive]): Process[PathErr2
}
