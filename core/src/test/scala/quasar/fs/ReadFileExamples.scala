package quasar
package fs

import quasar.Predef._
import quasar.fp._

import org.specs2.{scalaz => _, _}
import org.specs2.execute._
import org.specs2.specification._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

trait ReadFileExamples { self: mutable.Specification =>
  import ReadFile._, ReadError._, PathError2._
  import ReadFileExamples._

  val readFile = Ops[ReadFileF]
  import readFile._

  ////

  /** Interpret the given [[ReadFile]] program into task. */
  def interpret: ReadFile ~> Task

  /** Load the given data such that it is available to `interpret`. */
  def loadData(fs: List[TestDatum]): Task[Unit]

  /** Clear any data loaded by `loadData`. */
  def clearData: Task[Unit]

  ////

  def run: F ~> Task =
    new (F ~> Task) {
      def apply[A](fa: F[A]) = Free.runFC(fa)(interpret)
    }

  def runP[O](p: Process[M, O]): ReadErrT[Task, IndexedSeq[O]] =
    p.translate[ReadErrT[Task, ?]](Hoist[ReadErrT].hoist(run))
      .runLog[ReadErrT[Task, ?], O]

  implicit class RFExample(s: String) {
    def >>*[A: AsResult](fa: => F[A]): Example =
      s >> run(fa).run
  }

  // As these are read-only tests, we just load the data once
  step((clearData *> loadData(List(emptyFile, smallFile, largeFile, veryLongFile))).run)

  "ReadFile interpreter" should {
    "open returns FileNotFound when file DNE" >>* {
      val dne = dir("doesnt") </> file("exist")
      open(dne, Natural._0, None).run map { r =>
        r.toEither must beLeft(PathError(FileNotFound(dne)))
      }
    }

    "read unopened file handle returns UnknownHandle" >>* {
      val h = ReadHandle(42)
      read(h).run map { r =>
        r.toEither must beLeft(UnknownHandle(h))
      }
    }

    "read closed file handle returns UnknownHandle" >>* {
      val r = for {
        h  <- open(smallFile.file, Natural._0, None)
        _  <- close(h).liftM[ReadErrT]
        xs <- read(h)
      } yield xs

      r.run map (_ must beLike {
        case -\/(e) => e.fold(_ => ko("expected unknown handle"), _ => ok)
      })
    }

    "scan with offset zero and no limit reads entire file" >> {
      val r = runP(scan(smallFile.file, Natural._0, None))
      r.run.run.toEither must beRight(smallFile.data.toIndexedSeq)
    }

    "scan with offset k > 0 and no limit skips first k data" >> {
      val k = Natural._9 * Natural._2
      val r = runP(scan(smallFile.file, k, None))

      r.run.run.toEither must beRight(smallFile.data.drop(k.run.toInt).toIndexedSeq)
    }

    "scan with offset zero and limit j stops after j data" >> {
      val j = Positive._5
      val r = runP(scan(smallFile.file, Natural._0, Some(j)))

      r.run.run.toEither must beRight(smallFile.data.take(j.run.toInt).toIndexedSeq)
    }

    "scan with offset k and limit j takes j data, starting from k" >> {
      val j = Positive._5 * Positive._5 * Positive._5
      val r = runP(scan(largeFile.file, Natural.fromPositive(j), Some(j)))
      val d = largeFile.data.drop(j.run.toInt).take(j.run.toInt).toIndexedSeq

      r.run.run.toEither must beRight(d)
    }

    "scan with offset zero and limit j, where j > |file|, stops at end of file" >> {
      val j = Positive._5 * Positive._5 * Positive._5
      val r = runP(scan(smallFile.file, Natural._0, Some(j)))

      (j.run.toInt must beGreaterThan(smallFile.data.length)) and
      (r.run.run.toEither must beRight(smallFile.data.toIndexedSeq))
    }

    // TODO: What is the expected behavior here?
    "scan with offset k, where k > |file|, and no limit ???" >> todo

    "scan very long file is stack-safe" >> {
      runP(scanAll(veryLongFile.file).foldMap(_ => 1))
        .run.run.toEither must beRight(List(veryLongFile.data.length).toIndexedSeq)
    }

    // TODO: This was copied from existing tests, but what is being tested?
    "scan very long file twice" >> {
      val r = runP(scanAll(veryLongFile.file).foldMap(_ => 1))
      val l = List(veryLongFile.data.length).toIndexedSeq

      (r.run.run.toEither must beRight(l)) and (r.run.run.toEither must beRight(l))
    }
  }

  step(clearData.run)
}

object ReadFileExamples {
  final case class TestDatum(file: RelFile[Sandboxed], data: Stream[Data])

  val emptyFile = TestDatum(
    file("empty"),
    Stream())

  val smallFile = TestDatum(
    dir("testfiles") </> file("small"),
    Stream.range(1, 100) map (Data.Int(_)))

  val largeFile = {
    val sizeInMb = 10.0
    val bytesPerDoc = 750
    val numDocs = (sizeInMb * 1024 * 1024 / bytesPerDoc).toInt

    def jsonTree(depth: Int): Data =
      if (depth == 0)
        Data.Arr(Data.Str("abc") :: Data.Int(123) :: Data.Str("do, re, mi") :: Nil)
      else
        Data.Obj(ListMap("left" -> jsonTree(depth-1), "right" -> jsonTree(depth-1)))

    def json(i: Int) =
      Data.Obj(ListMap("seq" -> Data.Int(i), "filler" -> jsonTree(3)))

    TestDatum(
      dir("testfiles") </> file("large"),
      Stream.range(1, numDocs) map (json))
  }

  val veryLongFile = TestDatum(
    dir("testfiles") </> dir("length") </> file("very.long"),
    Stream.range(1, 1000000) map (Data.Int(_)))
}
