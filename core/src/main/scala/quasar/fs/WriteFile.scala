package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.stream._
import pathy.Path._

sealed trait WriteFile[A]

object WriteFile {
  final case class WriteHandle(run: Long) extends AnyVal

  final case class Open(dst: RelFile[Sandboxed]) extends WriteFile[PathError2 \/ WriteHandle]
  final case class Write(h: WriteHandle, data: Vector[Data]) extends WriteFile[Vector[WriteError]]
  final case class Close(h: WriteHandle) extends WriteFile[Unit]
}

object WriteFiles {
  implicit def apply[S[_]: Functor : (WriteFileF :<: ?[_])]: WriteFiles[S] =
    new WriteFiles[S]
}

final class WriteFiles[S[_]](implicit S0: Functor[S], S1: WriteFileF :<: S) {
  import WriteFile._
  import FileSystem.MoveSemantics

  type F[A] = Free[S, A]
  type M[A] = PathErr2T[F, A]

  /** Returns a channel that appends chunks of data to the given file, creating
    * it if it doesn't exist. Any errors encountered while writing are emitted,
    * all attempts are made to continue consuming input until the source is
    * exhausted.
    *
    * TODO: Test that the `close` finalizer runs as expected, when the source ends.
    */
  def appendChannel(dst: RelFile[Sandboxed]): Channel[M, Vector[Data], Vector[WriteError]] = {
    def append0(wh: WriteHandle): Vector[Data] => M[Vector[WriteError]] =
      ds => lift(Write(wh, ds)).liftM[PathErr2T]

    def close0(wh: WriteHandle): Process[M, Nothing] =
      Process.eval_[M, Unit](lift(Close(wh)).liftM[PathErr2T])

    Process.await(EitherT(lift(Open(dst))): M[WriteHandle])(wh =>
      channel.lift(append0(wh)).onComplete(close0(wh)))
  }

  /** Appends data to the given file in chunks, creating it if it doesn't exist.
    * An error is emitted whenever writing a particular `Data` fails. Consumes the
    * src until it is exhausted.
    */
  def appendChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]]): Process[M, WriteError] =
    src.translate[M](liftMT[F, PathErr2T])
      .through(appendChannel(dst))
      .flatMap(Process.emitAll(_))

  /** Appends data to the given file, creating it if it doesn't exist. May not
    * write all values from `src` in the presence of errors, will emit a
    * [[WriteError]] for each input value that failed to write.
    */
  def append(dst: RelFile[Sandboxed], src: Process[F, Data]): Process[M, WriteError] =
    appendChunked(dst, src map (Vector(_)))

  /** Write the data stream to the given path, replacing any existing contents,
    * atomically. Any errors during writing will abort the entire operation
    * leaving any existing values unaffected.
    */
  def saveChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]])
                 (implicit FS: FileSystems[S])
                 : Process[M, Unit] = {

    saveChunked0(dst, src, MoveSemantics.Overwrite)
  }

  def save(dst: RelFile[Sandboxed], src: Process[F, Data])
          (implicit FS: FileSystems[S])
          : Process[M, Unit] = {

    saveChunked(dst, src map (Vector(_)))
  }

  /** Create the given file with the contents of `src`. Fails if already exists. */
  def create(dst: RelFile[Sandboxed], src: Process[F, Data])
            (implicit FS: FileSystems[S])
            : Process[M, Unit] = {

    type G[E, A] = EitherT[F, E, A]

    // TODO: fill in proper case
    def shouldNotExist: M[Unit] = ???
      //MonadError[G, PathError2].raiseError(???)

    FS.fileExists(dst).liftM[Process].ifM(
      shouldNotExist.liftM[Process],
      saveChunked0(dst, src map (Vector(_)), MoveSemantics.FailIfExists))
  }

  /** Replace the contents of the given file with `src`. Fails if the file
    * doesn't exist.
    */
  def replace(dst: RelFile[Sandboxed], src: Process[F, Data]): Process[M, Unit] = ???
  // check exists, write to tmp, check again, move else error

  ////

  private def saveChunked0(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]], sem: MoveSemantics)
                          (implicit FS: FileSystems[S])
                          : Process[M, Unit] = {
    for {
      tmp  <- FS.tempFile.liftM[PathErr2T].liftM[Process]
      werr <- appendChunked(tmp, src).terminated.take(1)
      _    <- werr.cata(κ(FS.deleteFile(tmp)), FS.moveFile(tmp, dst, sem)).liftM[Process]
    } yield ()
  }

  private def lift[A](wf: WriteFile[A]): F[A] =
    Free.liftF(S1.inj(Coyoneda.lift(wf)))
}

