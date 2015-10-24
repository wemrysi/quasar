package quasar
package fs

import quasar.Predef._
import quasar.fp._

import org.specs2.mutable.Specification
import scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import pathy.Path._

import FileSystemTest._

abstract class FileSystemTest[S[_]: Functor](
  fss: FileSystemUT[S]*)(
  implicit S0: ReadFileF :<: S, S1: WriteFileF :<: S, S2: ManageFileF :<: S)

  extends Specification {

  type F[A]      = Free[S, A]
  type FsTask[A] = FileSystemErrT[Task, A]
  type Run       = F ~> Task

  val read   = ReadFile.Ops[S]
  val write  = WriteFile.Ops[S]
  val manage = ManageFile.Ops[S]

  def fileSystemShould(examples: Run => Unit): Unit =
    fss foreach { case FileSystemUT(name, f, prefix) =>
      s"$name FileSystem" should examples(hoistFree(f compose chroot.fileSystem[S](prefix)))
    }

  def runT(run: Run): FileSystemErrT[F, ?] ~> FsTask =
    Hoist[FileSystemErrT].hoist(run)

  def runLog[A](run: Run, p: Process[F, A]): Task[IndexedSeq[A]] =
    p.translate[Task](run).runLog

  def runLogT[A](run: Run, p: Process[FileSystemErrT[F, ?], A]): FsTask[IndexedSeq[A]] =
    p.translate[FsTask](runT(run)).runLog
}

object FileSystemTest {
  /** FileSystem Under Test */
  final case class FileSystemUT[S[_]](name: String, f: S ~> Task, testPrefix: AbsDir[Sandboxed])
}
