package quasar
package fs

import quasar.Predef._
import quasar.fp._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import pathy.Path._

class ReadFileSpec extends Specification with ScalaCheck {
  import inmemory._, DataGen._

  type F[A] = Free[FileSystem, A]
  type G[A] = StateT[Task, InMemState, A]
  type M[A] = FileSystemErrT[G, A]

  val testDir = dir("readfile") </> dir("spec")
  val rFile = ReadFile.Ops[FileSystem]
  val wFile = WriteFile.Ops[FileSystem]

  val hoistFs: InMemoryFs ~> G =
    Hoist[StateT[?[_], InMemState, ?]].hoist(pointNT[Task])

  val run: F ~> G =
    hoistFs compose[F] hoistFree(interpretFileSystem(readFile, writeFile, manageFile))

  val runT: FileSystemErrT[F, ?] ~> M =
    Hoist[FileSystemErrT].hoist(run)

  "ReadFile" should {
    "scan should read data until an empty vector is received" ! prop { xs: List[Data] =>
      xs.nonEmpty ==> {
        val f = testDir </> file("foo")
        val p = wFile.append(f, Process(xs: _*)).drain ++ rFile.scanAll(f)

        p.translate[M](runT).runLog.map(_.toList)
          .run.eval(InMemState.empty)
          .run.toEither must beRight(xs)
      }
    }

    "scan should automatically close the read handle when terminated early" ! prop { xs: List[Data] =>
      xs.nonEmpty ==> {
        val f = testDir </> file("bar")
        val n = xs.length / 2
        val p = wFile.append(f, Process(xs: _*)).drain ++ rFile.scanAll(f).take(n)

        p.translate[M](runT).runLog.map(_.toList)
          .run.leftMap(_.rm)
          .run(InMemState.empty)
          .run must_== ((Map.empty, \/.right(xs take n)))
      }
    }
  }
}
