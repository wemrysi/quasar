package quasar
package fs

import quasar.Predef._
import quasar.fp._

import monocle.std.{disjunction => D}

import org.specs2.execute._
import org.specs2.specification._

import pathy.Path._

import scala.annotation.tailrec

import scalaz.{EphemeralStream => EStream, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

class FileSystemSpec extends FileSystemTest[FileSystem](FileSystemSpec.inMemUT) {
  import FileSystemSpec._, FileSystemError._, PathError2._
  import ReadFile._

  sequential

  implicit class FSExample(s: String) {
    def >>*[A: AsResult](fa: => F[A])(implicit run: Run): Example =
      s >> run(fa).run
  }

  def deleteReadOnly: FileSystemErrT[F, Unit] =
    manage.deleteDir(readOnlyPrefix)

  def loadReadOnly: Process[FileSystemErrT[F, ?], Unit] = {
    type P[A] = Process[write.M, A]

    def loadDatum(td: TestDatum) = {
      val src = chunkStream(td.data, 1000)
        .foldRight(Process.halt: Process0[Vector[Data]])(xs => p => Process.emit(xs) ++ p)
      write.appendChunked(td.file, src)
    }

    List(emptyFile, smallFile, largeFile, veryLongFile)
      .foldMap(loadDatum)(PlusEmpty[P].monoid)
      .drain
  }

  fileSystemShould { implicit run =>
    "Reading Files" should {
      // Load read-only data
      step((runT(run)(deleteReadOnly) *> loadReadOnly.translate[FsTask](runT(run)).run)
            .run.void.run)

      "open returns FileNotFound when file DNE" >>* {
        val dne = rootDir </> dir("doesnt") </> file("exist")
        read.open(dne, Natural._0, None).run map { r =>
          r.toEither must beLeft(PathError(FileNotFound(dne)))
        }
      }

      "read unopened file handle returns UnknownReadHandle" >>* {
        val h = ReadHandle(42)
        read.read(h).run map { r =>
          r.toEither must beLeft(UnknownReadHandle(h))
        }
      }

      "read closed file handle returns UnknownReadHandle" >>* {
        val r = for {
          h  <- read.open(smallFile.file, Natural._0, None)
          _  <- read.close(h).liftM[FileSystemErrT]
          xs <- read.read(h)
        } yield xs

        r.run map { x =>
          (D.left composePrism unknownReadHandle).isMatching(x) must beTrue
        }
      }

      "scan with offset zero and no limit reads entire file" >> {
        val r = runLogT(run, read.scan(smallFile.file, Natural._0, None))
        r.run.run.toEither must beRight(smallFile.data.toIndexedSeq)
      }

      "scan with offset k > 0 and no limit skips first k data" >> {
        val k = Natural._9 * Natural._2
        val r = runLogT(run, read.scan(smallFile.file, k, None))
        val d = smallFile.data.zip(EStream.iterate(0)(_ + 1))
                  .dropWhile(_._2 < k.run.toInt).map(_._1)

        r.run.run.toEither must beRight(d.toIndexedSeq)
      }

      "scan with offset zero and limit j stops after j data" >> {
        val j = Positive._5
        val r = runLogT(run, read.scan(smallFile.file, Natural._0, Some(j)))

        r.run.run.toEither must beRight(smallFile.data.take(j.run.toInt).toIndexedSeq)
      }

      "scan with offset k and limit j takes j data, starting from k" >> {
        val j = Positive._5 * Positive._5 * Positive._5
        val r = runLogT(run, read.scan(largeFile.file, Natural.fromPositive(j), Some(j)))
        val d = largeFile.data.zip(EStream.iterate(0)(_ + 1))
                  .dropWhile(_._2 < j.run.toInt).map(_._1)
                  .take(j.run.toInt)

        r.run.run.toEither must beRight(d.toIndexedSeq)
      }

      "scan with offset zero and limit j, where j > |file|, stops at end of file" >> {
        val j = Positive._5 * Positive._5 * Positive._5
        val r = runLogT(run, read.scan(smallFile.file, Natural._0, Some(j)))

        (j.run.toInt must beGreaterThan(smallFile.data.length)) and
        (r.run.run.toEither must beRight(smallFile.data.toIndexedSeq))
      }

      // TODO: What is the expected behavior here?
      "scan with offset k, where k > |file|, and no limit ???" >> todo

      "scan very long file is stack-safe" >> {
        runLogT(run, read.scanAll(veryLongFile.file).foldMap(_ => 1))
          .run.run.toEither must beRight(List(veryLongFile.data.length).toIndexedSeq)
      }

      // TODO: This was copied from existing tests, but what is being tested?
      "scan very long file twice" >> {
        val r = runLogT(run, read.scanAll(veryLongFile.file).foldMap(_ => 1))
        val l = List(veryLongFile.data.length).toIndexedSeq

        (r.run.run.toEither must beRight(l)) and (r.run.run.toEither must beRight(l))
      }

      step(deleteReadOnly)
    }

    "Writing Files" should {
    }

    "Managing Files" should {
    }; ()
  }
}

object FileSystemSpec {
  import FileSystemTest._

  val InMem: Task[FileSystem ~> Task] =
    inmemory.runStatefully map { f =>
      f compose interpretFileSystem(
                  inmemory.readFile,
                  inmemory.writeFile,
                  inmemory.manageFile)
    }

  def inMemUT = FileSystemUT("In-Memory", InMem.run, rootDir)

  final case class TestDatum(file: AbsFile[Sandboxed], data: EStream[Data])

  val readOnlyPrefix: AbsDir[Sandboxed] = rootDir </> dir("readonly")

  val emptyFile = TestDatum(
    readOnlyPrefix </> file("empty"),
    EStream())

  val smallFile = TestDatum(
    readOnlyPrefix </> file("small"),
    EStream.range(1, 100) map (Data.Int(_)))

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
      readOnlyPrefix </> file("large"),
      EStream.range(1, numDocs) map (json))
  }

  val veryLongFile = TestDatum(
    readOnlyPrefix </> dir("length") </> file("very.long"),
    EStream.range(1, 100000) map (Data.Int(_)))

  ////

  private def chunkStream[A](s: EStream[A], size: Int): EStream[Vector[A]] = {
    @tailrec
    def chunk0(xs: EStream[A], v: Vector[A], i: Int): (EStream[A], Vector[A]) =
      if (i == 0)
        (xs, v)
      else
        (xs.headOption, xs.tailOption) match {
          case (Some(a), Some(xss)) =>
            chunk0(xss, v :+ a, i - 1)
          case (Some(a), None) =>
            (EStream(), v :+ a)
          case _ =>
            (EStream(), v)
        }

    EStream.unfold(chunk0(s, Vector(), size)) { case (ys, v) =>
      if (v.isEmpty) None else Some((v, chunk0(ys, Vector(), size)))
    }
  }
}
