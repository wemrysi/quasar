package quasar
package fs

import quasar.Predef._
import quasar.fp._
import quasar.Backend.ProcessingError

import scalaz._
import scalaz.syntax.monad._
import scalaz.stream._
import pathy.Path._

sealed trait WriteFile[A]

object WriteFile {
  sealed trait WriteMode

  object WriteMode {
    /** Indicates that written values will replace any existing values at
      * the destination, atomically.
      */
    case object Replace extends WriteMode

    /** Indicates that written values will extend the existing values at
      * the destination, may be partial in the presence of errors.
      */
    case object Append extends WriteMode
  }

  val modeReplace: WriteMode = WriteMode.Replace
  val modeAppend: WriteMode = WriteMode.Append

  final case class WriteHandle(run: Long) extends AnyVal

  final case class Open(path: RelFile[Sandboxed], mode: WriteMode) extends WriteFile[PathError2 \/ WriteHandle]
  final case class Write(h: WriteHandle, data: Vector[Data]) extends WriteFile[Vector[WriteError]]
  final case class Close(h: WriteHandle) extends WriteFile[Unit]
}

object WriteFiles {
  implicit def apply[S[_]: Functor : (WriteFileF :<: ?[_])]: WriteFiles[S] =
    new WriteFiles[S]
}

final class WriteFiles[S[_]: Functor : (WriteFileF :<: ?[_])] {
  import WriteFile._

  type F[A] = Free[S, A]
  type M[A] = PathErr2T[F, A]

  def writeChannel(path: RelFile[Sandboxed], mode: WriteMode): Channel[M, Vector[Data], Vector[WriteError]] = {
    def write0(wh: WriteHandle): Vector[Data] => M[Vector[WriteError]] =
      ds => lift(Write(wh, ds)).liftM[PathErr2T]

    def close0(wh: WriteHandle): Process[M, Nothing] =
      Process.eval_[M, Unit](lift(Close(wh)).liftM[PathErr2T])

    Process.await(EitherT(lift(Open(path, mode))): M[WriteHandle])(wh =>
      channel.lift(write0(wh)).onComplete(close0(wh)))
  }

  def writeChunked(path: RelFile[Sandboxed], src: Process[F, Vector[Data]], mode: WriteMode): Process[M, WriteError] =
    src.translate[M](liftMT[F, PathErr2T])
      .through(writeChannel(path, mode))
      .flatMap(Process.emitAll(_))

  def write(path: RelFile[Sandboxed], src: Process[F, Data], mode: WriteMode): Process[M, WriteError] =
    writeChunked(path, src.map(Vector(_)), mode)

  // NB: Supposed to have atomic semantics, requires Dir
  //
  // Should fail if path already exists
  //
  // Do we try and COW as a generic impl? Or just make this and `replace`
  // primitives to allow backends to optimize?
  //
  def create(path: RelFile[Sandboxed], src: Process[F, Data]): OptionT[F, ProcessingError] = ???

  // NB: Supposed to have atomic semantics, requires Dir
  //
  // Should fail if path DNE
  //
  def replace(path: RelFile[Sandboxed], src: Process[F, Data]): OptionT[F, ProcessingError] = ???

  def append(path: RelFile[Sandboxed], src: Process[F, Data]): Process[M, WriteError] =
    write(path, src, modeAppend)

  ////

  private def lift[A](wf: WriteFile[A]): F[A] =
    Free.liftF(Inject[WriteFileF, S].inj(Coyoneda.lift(wf)))
}

