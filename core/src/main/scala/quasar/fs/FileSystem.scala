package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scalaz._
import scalaz.std.option._
import scalaz.syntax.applicative._
import pathy.{Path => PPath}, PPath._

sealed trait FileSystem[A]

object FileSystem {
  sealed trait MoveSemantics {
    import MoveSemantics._

    def fold[X](overwrite: => X, failIfExists: => X, failIfMissing: => X): X =
      this match {
        case Overwrite0     => overwrite
        case FailIfExists0  => failIfExists
        case FailIfMissing0 => failIfMissing
      }
  }

  /** NB: Certain write operations' consistency is affected by faithful support
    *     of these semantics, thus their consistency/atomicity is as good as the
    *     support of these semantics by the interpreter.
    *
    *     Currently, this allows us to implement all the write scenarios in terms
    *     of append and move, however if this proves too difficult to support by
    *     backends, we may want to relax the move semantics and instead add
    *     additional primitive operations for the conditional write operations.
    */
  object MoveSemantics {
    private case object Overwrite0 extends MoveSemantics
    private case object FailIfExists0 extends MoveSemantics
    private case object FailIfMissing0 extends MoveSemantics

    /** Indicates the move operation should overwrite anything at the
      * destination, creating it if it doesn't exist.
      */
    val Overwrite: MoveSemantics = Overwrite0

    /** Indicates the move should (atomically, if possible) fail if the
      * exists.
      */
    val FailIfExists: MoveSemantics = FailIfExists0

    /** Indicates the move should (atomically, if possible) fail unless
      * the destination exists, overwriting it otherwise.
      */
    val FailIfMissing: MoveSemantics = FailIfMissing0
  }

  sealed trait MoveScenario {
    import MoveScenario._

    def fold[X](
      d2d: (RelDir[Sandboxed], RelDir[Sandboxed]) => X,
      f2f: (RelFile[Sandboxed], RelFile[Sandboxed]) => X
    ): X =
      this match {
        case DirToDir0(sd, dd)   => d2d(sd, dd)
        case FileToFile0(sf, df) => f2f(sf, df)
      }
  }

  object MoveScenario {
    private final case class DirToDir0(src: RelDir[Sandboxed], dst: RelDir[Sandboxed])
      extends MoveScenario
    private final case class FileToFile0(src: RelFile[Sandboxed], dst: RelFile[Sandboxed])
      extends MoveScenario

    val DirToDir: (RelDir[Sandboxed], RelDir[Sandboxed]) => MoveScenario =
      DirToDir0(_, _)

    val FileToFile: (RelFile[Sandboxed], RelFile[Sandboxed]) => MoveScenario =
      FileToFile0(_, _)
  }

  sealed trait Node {
    import Node._

    def fold[X](
      mnt: RelDir[Sandboxed] => X,
      pln: RelPath[Sandboxed] => X
    ): X =
      this match {
        case Mount0(d) => mnt(d)
        case Plain0(p) => pln(p)
      }

    def dir: Option[RelDir[Sandboxed]] =
      fold(some, _.swap.toOption)

    def file: Option[RelFile[Sandboxed]] =
      fold(κ(none[RelFile[Sandboxed]]), _.toOption)

    def path: RelDir[Sandboxed] \/ RelFile[Sandboxed] =
      fold(\/.left, ι)
  }

  object Node {
    private final case class Mount0(d: RelDir[Sandboxed]) extends Node
    private final case class Plain0(p: RelPath[Sandboxed]) extends Node

    val Mount: RelDir[Sandboxed] => Node =
      Mount0(_)

    val Plain: RelPath[Sandboxed] => Node =
      Plain0(_)

    val Dir: RelDir[Sandboxed] => Node =
      Plain compose \/.left

    val File: RelFile[Sandboxed] => Node =
      Plain compose \/.right
  }

  final case class Move(scenario: MoveScenario, semantics: MoveSemantics) extends FileSystem[PathError2 \/ Unit]
  final case class Delete(path: RelPath[Sandboxed]) extends FileSystem[PathError2 \/ Unit]
  final case class ListContents(dir: RelDir[Sandboxed]) extends FileSystem[PathError2 \/ Set[Node]]
  final case class TempFile(nearTo: Option[RelFile[Sandboxed]]) extends FileSystem[RelFile[Sandboxed]]

  final class Ops[S[_]](implicit S0: Functor[S], S1: FileSystemF :<: S) {
    type F[A] = Free[S, A]
    type M[A] = PathErr2T[F, A]

    /** Request the given move scenario be applied to the file system, using the
      * given semantics.
      */
    def move(scenario: MoveScenario, semantics: MoveSemantics): M[Unit] =
      EitherT(lift(Move(scenario, semantics)))

    /** Move the `src` dir to `dst` dir, requesting the semantics described by `sem`. */
    def moveDir(src: RelDir[Sandboxed], dst: RelDir[Sandboxed], sem: MoveSemantics): M[Unit] =
      move(MoveScenario.DirToDir(src, dst), sem)

    /** Move the `src` file to `dst` file, requesting the semantics described by `sem`. */
    def moveFile(src: RelFile[Sandboxed], dst: RelFile[Sandboxed], sem: MoveSemantics): M[Unit] =
      move(MoveScenario.FileToFile(src, dst), sem)

    /** Rename the `src` file in the same directory. */
    def renameFile(src: RelFile[Sandboxed], name: String): M[RelFile[Sandboxed]] = {
      val dst = PPath.renameFile(src, κ(FileName(name)))
      moveFile(src, dst, MoveSemantics.Overwrite).as(dst)
    }

    /** Delete the given file system path, fails if the path does not exist. */
    def delete(path: RelPath[Sandboxed]): M[Unit] =
      EitherT(lift(Delete(path)))

    /** Delete the given directory, fails if the directory does not exist. */
    def deleteDir(dir: RelDir[Sandboxed]): M[Unit] =
      delete(\/.left(dir))

    /** Delete the given file, fails if the file does not exist. */
    def deleteFile(file: RelFile[Sandboxed]): M[Unit] =
      delete(\/.right(file))

    /** Returns immediate children of the given directory, fails if the
      * directory does not exist.
      */
    def ls(dir: RelDir[Sandboxed]): M[Set[Node]] =
      EitherT(lift(ListContents(dir)))

    /** The children of the root directory. */
    def ls: M[Set[Node]] =
      ls(currentDir[Sandboxed])

    /** Returns the children of the given directory and all of their
      * descendants, fails if the directory does not exist.
      */
    def lsAll(dir: RelDir[Sandboxed]): M[Set[Node]] = {
      type S[A] = StreamT[M, A]

      def lsR(desc: RelDir[Sandboxed]): StreamT[M, Node] =
        StreamT.fromStream[M, Node](ls(dir </> desc) map (_.toStream))
          .flatMap(_.path.fold(
            d => lsR(desc </> d),
            f => Node.File(desc </> f).point[S]))

      lsR(currentDir[Sandboxed]).foldLeft(Set.empty[Node])(_ + _)
    }

    /** Returns whether the given file exists.
      *
      * TODO: This seems a bit sloppy w.r.t. equality, may want to normalize and
      *       convert to strings for comparision. Or see about adding an Equal[_]
      *       instance for pathy.Path intead of relying on Object#equals/hashCode.
      */
    def fileExists(file: RelFile[Sandboxed]): F[Boolean] = {
      // TODO: Add fileParent[B, S](f: Path[B, File, S]): Path[B, Dir, S] to pathy
      val parent =
        parentDir(file) getOrElse scala.sys.error("impossible, files have parents!")

      ls(parent)
        .map(_ flatMap (_.file map (parent </> _)) contains file)
        .getOrElse(false)
    }

    /** Returns the path to a new temporary file. When `nearTo` is specified,
      * an attempt is made to return a tmp path that is as physically close to
      * the given file as possible.
      */
    def tempFile(nearTo: Option[RelFile[Sandboxed]]): F[RelFile[Sandboxed]] =
      lift(TempFile(nearTo))

    /** Returns the path to a new temporary file. */
    def anyTempFile: F[RelFile[Sandboxed]] =
      tempFile(None)

    /** Returns the path to a new temporary file as physically close to the
      * specified file as possible.
      */
    def tempFileNear(file: RelFile[Sandboxed]): F[RelFile[Sandboxed]] =
      tempFile(Some(file))

    ////

    private def lift[A](fs: FileSystem[A]): F[A] =
      Free.liftF(S1.inj(Coyoneda.lift(fs)))
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: FileSystemF :<: S): Ops[S] =
      new Ops[S]
  }
}
