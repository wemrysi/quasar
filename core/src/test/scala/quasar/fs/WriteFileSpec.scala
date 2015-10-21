package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scalaz._
import scalaz.std.vector._
import pathy.Path._

class WriteFileSpec extends FileSystemSpec {
  import DataGen._, PathyGen._, FileSystemError._, PathError2._

  "WriteFile" should {

    "append should consume input and close write handle when finished" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data]) => xs.nonEmpty ==> {
        val p = write.appendF(f, xs).drain ++ read.scanAll(f)

        p.translate[M](runT).runLog.run
          .leftMap(_.wm)
          .run(emptyState)
          .run must_== ((Map.empty, \/.right(xs)))
      }
    }

    "append should aggregate all `PartialWrite` errors and emit the sum" >> todo

    "save should replace existing file" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val p = (write.appendF(f, xs) ++ write.saveF(f, ys)).drain ++ read.scanAll(f)

        evalLogZero(p).run.toEither must beRight(ys)
      }
    }

    "save should leave existing file untouched on failure" >> todo

    "create should fail if file exists" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val p = write.appendF(f, xs) ++ write.createF(f, ys)

        evalLogZero(p).run.toEither must beLeft(PathError(FileExists(f)))
      }
    }

    "create should consume all input into a new file" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data]) => xs.nonEmpty ==> {
        evalLogZero(write.createF(f, xs) ++ read.scanAll(f)).run.toEither must beRight(xs)
      }
    }

    "replace should fail if the file does not exist" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data]) => xs.nonEmpty ==> {
        evalLogZero(write.replaceF(f, xs)).run.toEither must beLeft(PathError(FileNotFound(f)))
      }
    }

    "replace should leave the existing file untouched on failure" >> todo

    "replace should overwrite the existing file with new data" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val p = write.saveF(f, xs) ++ write.replaceF(f, ys) ++ read.scanAll(f)

        evalLogZero(p).run.toEither must beRight(ys)
      }
    }
  }
}
