package quasar
package fs

import quasar.Predef._
import quasar.specs2._

import org.specs2.{scalaz => _, _}
import org.specs2.execute._
import org.specs2.specification._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

trait ReadFileExamples extends AroundEach with DisjunctionMatchers { self: mutable.Specification =>
  import ReadFile._, ReadError._
  private val readFile = Ops[ReadFileF]
  import readFile._

  type F[A] = Free[ReadFileF, A]

  /** Interpret the given [[ReadFile]] program into task. */
  def interpret: ReadFile ~> Task

  /** Load the given data such that it is available to `interpret`. */
  def loadData(m: Map[RelFile[Sandboxed], Vector[Data]]): Task[Unit]

  /** Clear any data loaded by `loadData`. */
  def clearData: Task[Unit]

  def run: F ~> Task =
    new (F ~> Task) {
      def apply[A](fa: F[A]) = Free.runFC(fa)(interpret)
    }

  def around[R: AsResult](r: => R): Result =
    AsResult(
      (loadData(ReadFileExamples.TestData) *> Task.delay(r))
        .onFinish(_ => clearData)
        .run)

  implicit class RFExample(s: String) {
    def >>*[A: AsResult](fa: => F[A]): Example =
      s >> run(fa).run
  }

  "ReadFile interpreter" should {
    "open returns FileNotFound when file DNE" >>* {
      val dne = currentDir </> dir("doesnt") </> file("exist")
      open(dne, Natural.zero, None).run map { r =>
        r must beLeftDisjunction(FileNotFound(dne))
      }
    }

    "read unopened file handle returns UnknownHandle" >>* {
      val h = ReadHandle(42)
      read(h).run map { r =>
        r must beLeftDisjunction(UnknownHandle(h))
      }
    }

    "read closed file handle returns UnknownHandle" >>* {
      val foo = currentDir[Sandboxed] </> file("foo.txt")

      val r = for {
        h  <- open(foo, Natural.zero, None)
        _  <- close(h).liftM[ReadErrT]
        xs <- read(h)
      } yield xs

      r.run map (_ must beLike {
        case -\/(e) => e.fold(_ => ko("expected unknown handle"), _ => ok)
      })
    }

    //"scan with zero offset and no limit reads entire file" >>* {
    //}
    // scan: zero offset and no limit reads entire file
    // scan: k offset and no limit skips first k items
    // scan: no offset and j limit stops after j items
    // scan: no offset and j limit stops at end of file when j > |file|
    // scan: k offset when k > |file| ???
    //
  }
}

object ReadFileExamples {
  val TestData = Map[RelFile[Sandboxed], Vector[Data]](
    (currentDir </> file("foo.txt"), Vector())
  )
}
