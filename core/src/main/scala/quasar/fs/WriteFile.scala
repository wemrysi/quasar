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
  import PathError2._
  import FileSystem.MoveSemantics

  type F[A]    = Free[S, A]
  type M[A]    = PathErr2T[F, A]
  type G[E, A] = EitherT[F, E, A]

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

  /** Same as `append` but accepts chunked [[Data]]. */
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

  /** Same as `save` but accepts chunked [[Data]]. */
  def saveChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]])
                 (implicit FS: FileSystems[S])
                 : Process[M, WriteError] = {

    saveChunked0(dst, src, MoveSemantics.Overwrite)
  }

  /** Write the data stream to the given path, replacing any existing contents,
    * atomically. Any errors during writing will abort the entire operation
    * leaving any existing values unaffected.
    */
  def save(dst: RelFile[Sandboxed], src: Process[F, Data])
          (implicit FS: FileSystems[S])
          : Process[M, WriteError] = {

    saveChunked(dst, src map (Vector(_)))
  }

  /** Same as `create` but accepts chunked [[Data]]. */
  def createChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]])
                   (implicit FS: FileSystems[S])
                   : Process[M, WriteError] = {

    def shouldNotExist: M[WriteError] =
      MonadError[G, PathError2].raiseError(PathExistsError(dst))

    FS.fileExists(dst).liftM[Process].ifM(
      shouldNotExist.liftM[Process],
      saveChunked0(dst, src, MoveSemantics.FailIfExists))
  }

  /** Create the given file with the contents of `src`. Fails if already exists. */
  def create(dst: RelFile[Sandboxed], src: Process[F, Data])
            (implicit FS: FileSystems[S])
            : Process[M, WriteError] = {

    createChunked(dst, src map (Vector(_)))
  }

  /** Same as `replace` but accepts chunked [[Data]]. */
  def replaceChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]])
                    (implicit FS: FileSystems[S])
                    : Process[M, WriteError] = {

    type G[E, A] = EitherT[F, E, A]

    def shouldExist: M[WriteError] =
      MonadError[G, PathError2].raiseError(PathMissingError(dst))

    FS.fileExists(dst).liftM[Process].ifM(
      saveChunked0(dst, src, MoveSemantics.FailIfMissing),
      shouldExist.liftM[Process])
  }

  /** Replace the contents of the given file with `src`. Fails if the file
    * doesn't exist.
    */
  def replace(dst: RelFile[Sandboxed], src: Process[F, Data])
             (implicit FS: FileSystems[S])
             : Process[M, WriteError] = {

    replaceChunked(dst, src map (Vector(_)))
  }

  ////

  private def saveChunked0(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]], sem: MoveSemantics)
                          (implicit FS: FileSystems[S])
                          : Process[M, WriteError] = {

    def cleanupTmp(tmp: RelFile[Sandboxed])(t: Throwable): Process[M, Nothing] =
      Process.eval_[M, Unit](FS.deleteFile(tmp)).causedBy(Cause.Error(t))

    FS.tempFileNear(dst).liftM[Process] flatMap { tmp =>
      appendChunked(tmp, src).terminated.take(1)
        .flatMap(_.cata(
          werr => FS.deleteFile(tmp).as(werr).liftM[Process],
          Process.eval_[M, Unit](FS.moveFile(tmp, dst, sem))))
        .onFailure(cleanupTmp(tmp))
    }
  }

  private def lift[A](wf: WriteFile[A]): F[A] =
    Free.liftF(S1.inj(Coyoneda.lift(wf)))
}

