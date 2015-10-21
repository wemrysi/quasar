package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scalaz._
import scalaz.std.vector._
import scalaz.syntax.functor._
import pathy.Path._

class WriteFileSpec extends FileSystemSpec {
  import DataGen._, PathyGen._, FileSystemError._, PathError2._

  "WriteFile" should {

    "append should consume input and close write handle when finished" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data]) => xs.nonEmpty ==> {
        val p = write.appendF(f, xs).drain ++ read.scanAll(f)

        p.translate[M](runT).runLog.run
          .leftMap(_.wm)
          .run(emptyMem)
          .run must_== ((Map.empty, \/.right(xs)))
      }
    }

    "append should aggregate all `PartialWrite` errors and emit the sum" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data]) => (xs.length > 1) ==> {
        val wf = WriteFailed(Data.Str("foo"), "b/c reasons")
        val ws = Vector(wf) +: xs.tail.as(Vector(PartialWrite(1)))

        runLogWithWrites(ws.toList, write.appendF(f, xs))
          .run.eval(emptyMem)
          .run.toEither must beRight(Vector(wf, PartialWrite(xs.length - 1)))
      }
    }

    "save should replace existing file" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val p = (write.appendF(f, xs) ++ write.saveF(f, ys)).drain ++ read.scanAll(f)

        evalLogZero(p).run.toEither must beRight(ys)
      }
    }

    "save should leave existing file untouched on failure" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val err = WriteFailed(Data.Str("bar"), "")
        val ws = (xs ++ ys.init).as(Vector()) :+ Vector(err)
        val p = (write.appendF(f, xs) ++ write.saveF(f, ys)).drain ++ read.scanAll(f)

        runLogWithWrites(ws.toList, p).run
          .leftMap(_.fm.keySet)
          .run(emptyMem)
          .run must_== ((Set(f), \/.right(xs)))
      }
    }

    "create should fail if file exists" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val p = write.appendF(f, xs) ++ write.createF(f, ys)

        evalLogZero(p).run.toEither must beLeft(PathError(FileExists(f)))
      }
    }

    "create should consume all input into a new file" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data]) => xs.nonEmpty ==> {
        evalLogZero(write.createF(f, xs) ++ read.scanAll(f))
          .run.toEither must beRight(xs)
      }
    }

    "replace should fail if the file does not exist" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data]) => xs.nonEmpty ==> {
        evalLogZero(write.replaceF(f, xs))
          .run.toEither must beLeft(PathError(FileNotFound(f)))
      }
    }

    "replace should leave the existing file untouched on failure" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val err = WriteFailed(Data.Int(42), "")
        val ws = (xs ++ ys.init).as(Vector()) :+ Vector(err)
        val p = (write.appendF(f, xs) ++ write.replaceF(f, ys)).drain ++ read.scanAll(f)

        runLogWithWrites(ws.toList, p).run
          .leftMap(_.fm.keySet)
          .run(emptyMem)
          .run must_== ((Set(f), \/.right(xs)))
      }
    }

    "replace should overwrite the existing file with new data" ! prop {
      (f: RelFile[Sandboxed], xs: Vector[Data], ys: Vector[Data]) => (xs.nonEmpty && ys.nonEmpty) ==> {
        val p = write.saveF(f, xs) ++ write.replaceF(f, ys) ++ read.scanAll(f)

        evalLogZero(p).run.toEither must beRight(ys)
      }
    }
  }
}
