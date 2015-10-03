package quasar

import quasar.Predef._
import quasar.fs.Path.PathError
import quasar.Backend.MoveSemantics

import pathy._, Path._
import scalaz._
import scalaz.Tags.{Multiplication => Mult}

final class Natural private (val run: Long) {
  def plus(other: Natural): Natural =
    new Natural(run + other.run)

  def + (other: Natural): Natural =
    plus(other)

  def times(other: Natural): Natural =
    new Natural(run * other.run)

  def * (other: Natural): Natural =
    times(other)
}

object Natural {
  def apply(n: Long): Option[Natural] =
    Some(n).filter(_ > 0).map(new Natural(_))

  val zero: Natural = new Natural(0)

  val one: Natural = new Natural(1)

  def fromPositive(n: Positive): Natural =
    new Natural(n.run)

  implicit val naturalAddition: Monoid[Natural] =
    Monoid.instance(_ + _, zero)

  implicit val naturalMultiplication: Monoid[Natural @@ Mult] =
    Monoid.instance(
      (x, y) => Mult(Mult.unwrap(x) * Mult.unwrap(y)),
      Mult(one))
}

final class Positive private (val run: Long) {
  def plus(other: Positive): Positive =
    new Positive(run + other.run)

  def + (other: Positive): Positive =
    plus(other)

  def times(other: Positive): Positive =
    new Positive(run * other.run)

  def * (other: Positive): Positive =
    times(other)
}

object Positive {
  def apply(n: Long): Option[Positive] =
    Some(n).filter(_ > 1).map(new Positive(_))

  val one: Positive = new Positive(1)

  implicit val positiveSemigroup: Semigroup[Positive] =
    Semigroup.instance(_ + _)

  implicit val positiveMultiplication: Monoid[Positive @@ Mult] =
    Monoid.instance(
      (x, y) => Mult(Mult.unwrap(x) * Mult.unwrap(y)),
      Mult(one))
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

