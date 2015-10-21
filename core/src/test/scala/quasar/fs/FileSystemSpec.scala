package quasar
package fs

import quasar.fp._
import scala.collection.IndexedSeq

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scalaz._
import scalaz.stream._
import scalaz.concurrent.Task

trait FileSystemSpec extends Specification with ScalaCheck {
  import inmemory._

  type F[A] = Free[FileSystem, A]
  type G[A] = StateT[Task, InMemState, A]
  type M[A] = FileSystemErrT[G, A]

  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val emptyState = InMemState.empty

  val hoistFs: InMemoryFs ~> G =
    Hoist[StateT[?[_], InMemState, ?]].hoist(pointNT[Task])

  val run: F ~> G =
    hoistFs compose[F] hoistFree(interpretFileSystem(readFile, writeFile, manageFile))

  val runT: FileSystemErrT[F, ?] ~> M =
    Hoist[FileSystemErrT].hoist(run)

  def runLog[A](p: Process[FileSystemErrT[F, ?], A]): M[IndexedSeq[A]] =
    p.translate[M](runT).runLog

  def evalLogZero[A](p: Process[FileSystemErrT[F, ?], A]): Task[FileSystemError \/ IndexedSeq[A]] =
    runLog(p).run.eval(emptyState)
}
