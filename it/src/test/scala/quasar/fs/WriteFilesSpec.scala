package quasar
package fs

import quasar.Predef._

import monocle.std.{disjunction => D}

import pathy.Path._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

class WriteFilesSpec extends FileSystemTest[FileSystem](FileSystemTest.allFsUT) {
  import FileSystemTest._, FileSystemError._
  import WriteFile._, ManageFile._

  val writesPrefix: AbsDir[Sandboxed] = rootDir </> dir("forwriting")

  def deleteForWriting(run: Run): FsTask[Unit] =
    runT(run)(manage.deleteDir(writesPrefix))

  fileSystemShould { implicit run =>
    "Writing Files" should {
      step(deleteForWriting(run).runVoid)

      "write to unknown handle returns UnknownWriteHandle" >>* {
        val h = WriteHandle(42)
        write.write(h, Vector()) map { r =>
          r must_== Vector(UnknownWriteHandle(h))
        }
      }

      "write to closed handle returns UnknownWriteHandle" >>* {
        val f = writesPrefix </> dir("d1") </> file("f1")
        val r = for {
          h    <- write.open(f)
          _    <- write.close(h).liftM[FileSystemErrT]
          errs <- write.write(h, Vector()).liftM[FileSystemErrT]
        } yield errs

        r.run.map { xs =>
          D.right
            .composeOptional(vectorFirst[FileSystemError])
            .composePrism(unknownWriteHandle) isMatching (xs) must beTrue
        }
      }

      "append should write data to file" >> {
        val f = writesPrefix </> file("saveone")
        val p = write.appendF(f, oneDoc).drain ++ read.scanAll(f)

        runLogT(run, p).runEither must beRight(oneDoc)
      }

      "append empty input should not result in a new file" >> {
        val f = writesPrefix </> file("emptyfile")
        val p = write.appendF(f, Vector[Data]()).drain ++
                (manage.fileExists(f).liftM[FileSystemErrT] : manage.M[Boolean]).liftM[Process]

        runLogT(run, p).run.run must_== \/.right(Vector(false))
      }

      "append two files, one in subdir of the other's parent, should succeed" >> {
        val d = writesPrefix </> dir("subdir1")
        val f1 = d </> file("subdirfile1")
        val f1Node = Node.File(file("subdirfile1"))
        val f2 = d </> dir("subdir2") </> file("subdirfile2")
        val f2Node = Node.File(dir("subdir2") </> file("subdirfile2"))
        val p = write.appendF(f1, oneDoc).drain ++
                write.appendF(f2, oneDoc).drain ++
                manage.lsAll(d).liftM[Process]

        runLogT(run, p).map(_.flatMap(_.toVector))
          .runEither must beRight(containTheSameElementsAs(List(f1Node, f2Node)))
      }

      step(deleteForWriting(run).runVoid)
    }; ()
  }
}
