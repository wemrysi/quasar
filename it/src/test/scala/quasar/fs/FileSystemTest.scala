package quasar
package fs

import quasar.Predef._
import quasar.fp._
import quasar.config.{BackendConfig, MongoDbConfig}
import quasar.physical.mongodb.{filesystems => mongofs}

import monocle.Optional
import monocle.function.Index
import monocle.std.vector._

import org.specs2.mutable.Specification
import org.specs2.execute._
import org.specs2.specification._

import pathy.Path._

import scala.Either

import scalaz.{EphemeralStream => EStream, Optional => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

/** Executes all the examples defined within the `fileSystemShould` block
  * for each file system in `fileSystems`.
  *
  * TODO: Currently, examples for a single filesystem are executed concurrently,
  *       but the suites themselves are executed sequentially due to the `step`s
  *       inserted for setup/teardown. It'd be nice if the tests for all
  *       filesystems would run concurrently.
  */
abstract class FileSystemTest[S[_]: Functor](
  val fileSystems: Task[NonEmptyList[FileSystemUT[S]]]
) extends Specification {

  args.report(showtimes = true)

  type F[A]      = Free[S, A]
  type FsTask[A] = FileSystemErrT[Task, A]
  type Run       = F ~> Task

  def fileSystemShould(examples: String => Run => Unit): Unit =
    fileSystems.map(_ foreach { case FileSystemUT(name, f, prefix) =>
      s"$name FileSystem" should examples(name)(hoistFree(f)); ()
    }).run

  def runT(run: Run): FileSystemErrT[F, ?] ~> FsTask =
    Hoist[FileSystemErrT].hoist(run)

  def runLog[A](run: Run, p: Process[F, A]): Task[IndexedSeq[A]] =
    p.translate[Task](run).runLog

  def runLogT[A](run: Run, p: Process[FileSystemErrT[F, ?], A]): FsTask[IndexedSeq[A]] =
    p.translate[FsTask](runT(run)).runLog

  def execT[A](run: Run, p: Process[FileSystemErrT[F, ?], A]): FsTask[Unit] =
    p.translate[FsTask](runT(run)).run

  ////

  implicit class FSExample(s: String) {
    def >>*[A: AsResult](fa: => F[A])(implicit run: Run): Example =
      s >> run(fa).run
  }

  implicit class RunFsTask[A](fst: FsTask[A]) {
    import Leibniz.===

    def runEither: Either[FileSystemError, A] =
      fst.run.run.toEither

    def runOption(implicit ev: A === Unit): Option[FileSystemError] =
      fst.run.run.swap.toOption

    def runVoid(implicit ev: A === Unit): Unit =
      fst.run.void.run
  }
}

object FileSystemTest {
  val oneDoc: Vector[Data] =
    Vector(Data.Obj(ListMap("a" -> Data.Int(1))))

  val anotherDoc: Vector[Data] =
    Vector(Data.Obj(ListMap("b" -> Data.Int(2))))

  def manyDocs(n: Int): EStream[Data] =
    EStream.range(0, n) map (n => Data.Obj(ListMap("a" -> Data.Int(n))))

  def vectorFirst[A]: Optional[Vector[A], A] =
    Index.index[Vector[A], Int, A](0)

  //--- FileSystems to Test ---

  def allFsUT: Task[NonEmptyList[FileSystemUT[FileSystem]]] =
    (inMemUT |@| externalFsUT) { (mem, ext) =>
      (mem <:: ext) map (ut => ut.contramap(chroot.fileSystem(ut.testDir)))
    }

  def externalFsUT = TestConfig.externalFileSystems {
    case (MongoDbConfig(cs), dir) =>
      lazy val f = mongofs.testFileSystem(cs, dir).run
      Task.delay(f)
  }

  val inMemUT: Task[FileSystemUT[FileSystem]] = {
    lazy val f = inmemory.runStatefully(inmemory.InMemState.empty)
                   .map(_ compose inmemory.fileSystem)
                   .run

    Task.delay(FileSystemUT("in-memory", f, rootDir))
  }
}
