package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scalaz._
import scalaz.std.option._
import scalaz.syntax.std.option._
import scalaz.syntax.applicative._
import pathy.Path._

/** TODO: Naming, not sure if this is the appropriate name... */
sealed trait FileSystem[A]

object FileSystem {
  sealed trait MoveSemantics {
    import MoveSemantics._

    def fold[X](ovr: => X, fail: => X): X =
      this match {
        case Overwrite0    => ovr
        case FailIfExists0 => fail
      }
  }

  object MoveSemantics {
    private case object Overwrite0 extends MoveSemantics
    private case object FailIfExists0 extends MoveSemantics

    val Overwrite: MoveSemantics = Overwrite0
    val FailIfExists: MoveSemantics = FailIfExists0
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

    def fold[X](mnt: RelDir[Sandboxed] => X, pln: RelPath[Sandboxed] => X): X =
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

  // TODO: How can these fail? What makes sense to include as part of the domain and part of the impl?
  final case class Move(scenario: MoveScenario, semantics: MoveSemantics) extends FileSystem[PathError2 \/ Unit]
  final case class Delete(path: RelPath[Sandboxed]) extends FileSystem[PathError2 \/ Unit]
  final case class ListContents(dir: RelDir[Sandboxed]) extends FileSystem[PathError2 \/ Set[Node]]
  // TODO: What sort of parameters might be useful here, maybe as hints for where to create temp file?
  case object TempFile extends FileSystem[RelFile[Sandboxed]]
}

object FileSystems {
  implicit def apply[S[_]](implicit S0: Functor[S], S1: FileSystemF :<: S): FileSystems[S] =
    new FileSystems[S]
}

final class FileSystems[S[_]](implicit S0: Functor[S], S1: FileSystemF :<: S) {
  import FileSystem._

  type F[A] = Free[S, A]
  type M[A] = PathErr2T[F, A]

  def moveDir(src: RelDir[Sandboxed], dst: RelDir[Sandboxed], sem: MoveSemantics): M[Unit] =
    EitherT(lift(Move(MoveScenario.DirToDir(src, dst), sem)))

  def moveFile(src: RelFile[Sandboxed], dst: RelFile[Sandboxed], sem: MoveSemantics): M[Unit] =
    EitherT(lift(Move(MoveScenario.FileToFile(src, dst), sem)))

  def deleteDir(dir: RelDir[Sandboxed]): M[Unit] =
    EitherT(lift(Delete(\/.left(dir))))

  def deleteFile(file: RelFile[Sandboxed]): M[Unit] =
    EitherT(lift(Delete(\/.right(file))))

  def ls(dir: RelDir[Sandboxed]): M[Set[Node]] =
    EitherT(lift(ListContents(dir)))

  def ls: M[Set[Node]]=
    ls(currentDir[Sandboxed])

  def lsAll(dir: RelDir[Sandboxed]): M[Set[Node]] = {
    type S[A] = StreamT[M, A]

    def lsR(desc: RelDir[Sandboxed]): StreamT[M, Node] =
      StreamT.fromStream[M, Node](ls(dir </> desc) map (_.toStream))
        .flatMap(_.path.fold(
          d => lsR(desc </> d),
          f => Node.File(desc </> f).point[S]))

    lsR(currentDir[Sandboxed]).foldLeft(Set.empty[Node])(_ + _)
  }

  def fileExists(file: RelFile[Sandboxed]): M[Boolean] =
    parentDir(file).cata(
      // NB: When in rome, I guess.
      dir => ls(dir) map (_ flatMap (_.file map (dir </> _)) contains file),
      // Files _must_ have a parent dir and the type guarantees a file.
      scala.sys.error("impossible!"))

  def tempFile: F[RelFile[Sandboxed]] =
    lift(TempFile)

  ////

  private def lift[A](fs: FileSystem[A]): F[A] =
    Free.liftF(S1.inj(Coyoneda.lift(fs)))
}
