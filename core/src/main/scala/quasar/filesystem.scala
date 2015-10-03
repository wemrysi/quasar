package quasar

import quasar.Predef._
import quasar.fs.Path.PathError
import quasar.Backend.MoveSemantics

import pathy._, Path._
import scalaz._

final case class Natural private (run: Long) {
  def plus(other: Natural): Natural =
    new Natural(run + other.run)

  def times(other: Natural): Natural =
    new Natural(run * other.run)
}

object Natural {
  def apply(n: Long): Option[Natural] =
    Some(n).filter(_ > 0).map(new Natural(_))

  val zero: Natural = new Natural(0)

  val one: Natural = new Natural(1)

  def fromPositive(n: Positive): Natural =
    new Natural(n.run)

  implicit val naturalAddition: Monoid[Natural] =
    Monoid.instance(_ plus _, zero)

  import Tags.{Multiplication => Mult}

  implicit val naturalMultiplication: Monoid[Natural @@ Mult] =
    Monoid.instance(
      (x, y) => Mult.wrap(Mult.unwrap(x) times Mult.unwrap(y)),
      Mult.wrap(one))
}

final case class Positive private (run: Long) {
  def plus(other: Positive): Positive =
    new Positive(run + other.run)

  def times(other: Positive): Positive =
    new Positive(run * other.run)
}

object Positive {
  def apply(n: Long): Option[Positive] =
    Some(n).filter(_ > 1).map(new Positive(_))

  val one: Positive = new Positive(1)

  implicit val positiveSemigroup: Semigroup[Positive] =
    Semigroup.instance(_ plus _)

  import Tags.{Multiplication => Mult}

  implicit val positiveMultiplication: Monoid[Positive @@ Mult] =
    Monoid.instance(
      (x, y) => Mult.wrap(Mult.unwrap(x) times Mult.unwrap(y)),
      Mult.wrap(one))
}

sealed trait WriteFile[A]

object WriteFile {
  // TODO: Need to support appending...maybe a write mode? Concat vs replace or similar?
  final case class WriteHandle(run: Long) extends AnyVal

  final case class Open(path: RelFile[Sandboxed]) extends WriteFile[WriteHandle]
  final case class Write(h: WriteHandle, data: Vector[Data]) extends WriteFile[Unit]
  final case class Close(h: WriteHandle) extends WriteFile[Unit]
}

  // FILES

  //final case class Scan(path: RelFile[Sandboxed], offset: Natural, limit: Option[Positive]) extends FileSystemF[Process[PathErrT[ReadFile, ?], Data]]

  // TODO: This isn't used anywhere yet, necessary?
  //final case class Size(path: RelFile[Sandboxed]) extends FileSystemF[PathError \/ Natural]

  //final case class Save(path: RelFile[Sandboxed]) extends FileSystemF[Sink[ProcErrT[WriteFile, ?], Data]]

sealed trait DirHandle[A]

object DirHandle {
  final case class Move(src: RelFile[Sandboxed], dst: RelFile[Sandboxed], semantics: MoveSemantics) extends DirHandle[Option[PathError]]
  final case class Delete(path: RelDir[Sandboxed] \/ RelFile[Sandboxed]) extends DirHandle[Option[PathError]]
  final case class List(dir: RelDir[Sandboxed]) extends DirHandle[Set[RelDir[Sandboxed] \/ RelFile[Sandboxed]]]
}

