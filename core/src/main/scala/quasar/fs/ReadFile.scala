package quasar
package fs

import quasar.Predef._

import scalaz._
import scalaz.syntax.monad._
import scalaz.stream._
import pathy.Path._

sealed trait ReadFile[A]

object ReadFile {
  final case class ReadHandle(run: Long) extends AnyVal

  final case class Open(path: RelFile[Sandboxed], offset: Natural, limit: Option[Positive]) extends ReadFile[PathError2 \/ ReadHandle]
  final case class Read(h: ReadHandle) extends ReadFile[Vector[Data]]
  final case class Close(h: ReadHandle) extends ReadFile[Unit]
}

object ReadFiles {
  implicit def apply[S[_]: Functor : (ReadFileF :<: ?[_])]: ReadFiles[S] =
    new ReadFiles[S]
}

final class ReadFiles[S[_]: Functor : (ReadFileF :<: ?[_])] {
  import ReadFile._

  type F[A] = Free[S, A]
  type M[A] = PathErr2T[F, A]

  def scan(path: RelFile[Sandboxed], offset: Natural, limit: Option[Positive]): Process[M, Data] =
    Process.await[M, ReadHandle, Data](EitherT(lift(Open(path, offset, limit))))(rh =>
      Process.repeatEval[M, Vector[Data]](lift(Read(rh)).liftM[PathErr2T])
        .flatMap(data => if (data.isEmpty) Process.halt else Process.emitAll(data))
        .onComplete(Process.eval_[M, Unit](lift(Close(rh)).liftM[PathErr2T])))

  def scanAll(path: RelFile[Sandboxed]): Process[M, Data] =
    scan(path, Natural.zero, None)

  def scanTo(path: RelFile[Sandboxed], limit: Positive): Process[M, Data] =
    scan(path, Natural.zero, Some(limit))

  def scanFrom(path: RelFile[Sandboxed], offset: Natural): Process[M, Data] =
    scan(path, offset, None)

  ////

  private def lift[A](rf: ReadFile[A]): F[A] =
    Free.liftF(Inject[ReadFileF, S].inj(Coyoneda.lift(rf)))
}
