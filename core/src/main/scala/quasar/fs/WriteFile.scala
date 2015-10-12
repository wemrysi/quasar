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

  final case class Open(file: RelFile[Sandboxed])
    extends WriteFile[WriteHandle]
  final case class Write(h: WriteHandle, chunk: Vector[Data])
    extends WriteFile[Vector[WriteError]]
  final case class Close(h: WriteHandle)
    extends WriteFile[Unit]

  final class Ops[S[_]](implicit S0: Functor[S], S1: WriteFileF :<: S) {
    import PathError2._
    import FileSystem.MoveSemantics

    type F[A]    = Free[S, A]
    type M[A]    = PathErr2T[F, A]
    type G[E, A] = EitherT[F, E, A]

    /** Returns a write handle for the specified file which may be used to
      * append data to the file it represents, creating it if necessary.
      *
      * Care must be taken to `close` the handle when it is no longer needed
      * to avoid potential resource leaks.
      */
    def open(file: RelFile[Sandboxed]): F[WriteHandle] =
      lift(Open(file))

    /** Write a chunk of data to the file represented by the write handle.
      *
      * Attempts to write as much of the chunk as possible, even if parts of
      * it fail, any such failures will be returned in the output [[Vector]].
      * An empty [[Vector]] means the entire chunk was written successfully.
      */
    def write(wh: WriteHandle, chunk: Vector[Data]): F[Vector[WriteError]] =
      lift(Write(wh, chunk))

    /** Close the write handle, freeing any resources it was using. */
    def close(wh: WriteHandle): F[Unit] =
      lift(Close(wh))

    /** Returns a channel that appends chunks of data to the given file, creating
      * it if it doesn't exist. Any errors encountered while writing are emitted,
      * all attempts are made to continue consuming input until the source is
      * exhausted.
      */
    def appendChannel(dst: RelFile[Sandboxed]): Channel[F, Vector[Data], Vector[WriteError]] = {
      def writeChunk(wh: WriteHandle): Vector[Data] => F[Vector[WriteError]] =
        write(wh, _)

      Process.await(open(dst))(wh =>
        channel.lift(writeChunk(wh)).onComplete(Process.eval_(close(wh))))
    }

    /** Same as `append` but accepts chunked [[Data]]. */
    def appendChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]]): Process[F, WriteError] =
      src through appendChannel(dst) flatMap Process.emitAll

    /** Appends data to the given file, creating it if it doesn't exist. May not
      * write all values from `src` in the presence of errors, will emit a
      * [[WriteError]] for each input value that failed to write.
      */
    def append(dst: RelFile[Sandboxed], src: Process[F, Data]): Process[F, WriteError] =
      appendChunked(dst, src map (Vector(_)))

    /** Same as `save` but accepts chunked [[Data]]. */
    def saveChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]])
                   (implicit FS: FileSystem.Ops[S])
                   : Process[M, WriteError] = {

      saveChunked0(dst, src, MoveSemantics.Overwrite)
    }

    /** Write the data stream to the given path, replacing any existing contents,
      * atomically. Any errors during writing will abort the entire operation
      * leaving any existing values unaffected.
      */
    def save(dst: RelFile[Sandboxed], src: Process[F, Data])
            (implicit FS: FileSystem.Ops[S])
            : Process[M, WriteError] = {

      saveChunked(dst, src map (Vector(_)))
    }

    /** Same as `create` but accepts chunked [[Data]]. */
    def createChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]])
                     (implicit FS: FileSystem.Ops[S])
                     : Process[M, WriteError] = {

      def shouldNotExist: M[WriteError] =
        MonadError[G, PathError2].raiseError(FileExists(dst))

      fileExistsM(dst).liftM[Process].ifM(
        shouldNotExist.liftM[Process],
        saveChunked0(dst, src, MoveSemantics.FailIfExists))
    }

    /** Create the given file with the contents of `src`. Fails if already exists. */
    def create(dst: RelFile[Sandboxed], src: Process[F, Data])
              (implicit FS: FileSystem.Ops[S])
              : Process[M, WriteError] = {

      createChunked(dst, src map (Vector(_)))
    }

    /** Same as `replace` but accepts chunked [[Data]]. */
    def replaceChunked(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]])
                      (implicit FS: FileSystem.Ops[S])
                      : Process[M, WriteError] = {

      def shouldExist: M[WriteError] =
        MonadError[G, PathError2].raiseError(FileNotFound(dst))

      fileExistsM(dst).liftM[Process].ifM(
        saveChunked0(dst, src, MoveSemantics.FailIfMissing),
        shouldExist.liftM[Process])
    }

    /** Replace the contents of the given file with `src`. Fails if the file
      * doesn't exist.
      */
    def replace(dst: RelFile[Sandboxed], src: Process[F, Data])
               (implicit FS: FileSystem.Ops[S])
               : Process[M, WriteError] = {

      replaceChunked(dst, src map (Vector(_)))
    }

    ////

    private def fileExistsM(file: RelFile[Sandboxed])
                           (implicit FS: FileSystem.Ops[S])
                           : M[Boolean] = {

      FS.fileExists(file).liftM[PathErr2T]
    }

    private def saveChunked0(dst: RelFile[Sandboxed], src: Process[F, Vector[Data]], sem: MoveSemantics)
                            (implicit FS: FileSystem.Ops[S])
                            : Process[M, WriteError] = {

      def cleanupTmp(tmp: RelFile[Sandboxed])(t: Throwable): Process[M, Nothing] =
        Process.eval_[M, Unit](FS.deleteFile(tmp)).causedBy(Cause.Error(t))

      FS.tempFileNear(dst).liftM[PathErr2T].liftM[Process] flatMap { tmp =>
        appendChunked(tmp, src).terminated.take(1)
          .translate[M](liftMT[F, PathErr2T])
          .flatMap(_.cata(
            werr => FS.deleteFile(tmp).as(werr).liftM[Process],
            Process.eval_[M, Unit](FS.moveFile(tmp, dst, sem))))
          .onFailure(cleanupTmp(tmp))
      }
    }

    private def lift[A](wf: WriteFile[A]): F[A] =
      Free.liftF(S1.inj(Coyoneda.lift(wf)))
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: WriteFileF :<: S): Ops[S] =
      new Ops[S]
  }
}
