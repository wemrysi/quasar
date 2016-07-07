/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.sparkcore.fs


import quasar.Predef._
import quasar.fp.free._
import quasar.fs.PathError._
import quasar.fs.FileSystemError._
import quasar.fs._
import quasar.fs.ManageFile.MoveSemantics

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.lang.System

import org.specs2.ScalaCheck
import org.specs2.scalaz._
import org.specs2.mutable.Specification
import pathy.Path._
import scalaz._, concurrent.Task

class ManageFileSpec extends Specification with ScalaCheck with DisjunctionMatchers  {

  type Eff[A] = Task[A]

  "managefile" should {
    "delete" should {
      "delete file on a local FS if exists" in {
        // given
        val path = tempFile()

        val program = define { unsafe =>
          for {
            _ <- unsafe.delete(path)
          } yield ()
        }
        // when
        exists(path) must_== true
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result  must_== \/-(())
        exists(path) must_== false
        ok
      }

      "fail to delete if file does NOT exist" in {
        // given
        val path = tempFile()

        val program = define { unsafe =>
          for {
            _ <- unsafe.delete(path)
          } yield ()
        }
        // when
        program.run.foldMap(interpreter).unsafePerformSync
        exists(path) must_== false
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must_== -\/((PathErr(InvalidPath(path, "File does not exist"))))
        ok
      }


      "fail to delete if folder not empty" in {
        // given
        val filePath = tempFile()
        val path = sandboxAbs(pathy.Path.parentDir(filePath).get)

        val program = define { unsafe =>
          for {
            _ <- unsafe.delete(path)
          } yield ()
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must_== -\/((PathErr(InvalidPath(path, "Directory is not empty"))))
        ok
      }
    }
    "tempFile" should {
      "create temp file neear existing path" in {
        // given
        val nearDir = tempDir()
        val program = define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearDir)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must beRightDisjunction.like {
          case path =>
            posixCodec.unsafePrintPath(path).startsWith(posixCodec.unsafePrintPath(nearDir)) must_== true
        }
        ok
      }
      
      "fail if near is a non existing directory" in {
        // given
        import pathy.Path._

        val nearDir = rootDir </> dir("nonexisting")
        val program = define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearDir)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must_== -\/((PathErr(InvalidPath(nearDir, s"Could not create temp file in dir $nearDir"))))
        ok
      }
      
      "fail if near a file (not a directory)" in {
        // given
        val nearDir = tempFile()
        val program = define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearDir)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must_== -\/((PathErr(
          InvalidPath(nearDir, s"Provided $nearDir is not a directory"))))
        ok
      }
       
    }

    "move" should {

      "move file from source to desination when semantics == overwrite & dst does not exist" in {
        // given
        val src = tempFile(maybeContent =  Some("some content"))
        val dst = tempFile(createFile = false)

        exists(src) must_== true
        exists(dst) must_== false

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        exists(src) must_== false
        exists(dst) must_== true
        getContent(dst) must_== List("some content")
        ok
      }

      "move file from source to desination when semantics == overwrite & dst does exist" in {
        // given
        val src = tempFile(maybeContent = Some("src content"))
        val dst = tempFile(maybeContent = Some("dst content"))

        exists(src) must_== true
        exists(dst) must_== true
        getContent(src) must_== List("src content")
        getContent(dst) must_== List("dst content")

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        exists(src) must_== false
        exists(dst) must_== true
        getContent(dst) must_== List("src content")
        ok
      }

      "move file from source to desination when semantics == failIfExists & dst does not exist" in {
        // given
        val src = tempFile(maybeContent =  Some("some content"))
        val dst = tempFile(createFile = false)

        exists(src) must_== true
        exists(dst) must_== false

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        exists(src) must_== false
        exists(dst) must_== true
        getContent(dst) must_== List("some content")
        ok
      }

      "fail when try yo move file from source to desination when semantics == failIfExists & dst does exist" in {
        // given
        val src = tempFile(maybeContent = Some("src content"))
        val dst = tempFile(maybeContent = Some("dst content"))

        exists(src) must_== true
        exists(dst) must_== true
        getContent(src) must_== List("src content")
        getContent(dst) must_== List("dst content")

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must_== -\/((PathErr(
          InvalidPath(src, "Can not move to destination that already exists if semnatics == failIfExists")))
        )
        exists(src) must_== true
        exists(dst) must_== true
        getContent(src) must_== List("src content")
        getContent(dst) must_== List("dst content")
        ok
      }

      "move file from source to desination when semantics == failIfMissing & dst does exist" in {
        // given
        val src = tempFile(maybeContent =  Some("src content"))
        val dst = tempFile(maybeContent = Some("dst content"))

        exists(src) must_== true
        exists(dst) must_== true
        getContent(src) must_== List("src content")
        getContent(dst) must_== List("dst content")

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        exists(src) must_== false
        exists(dst) must_== true
        getContent(dst) must_== List("src content")
        ok
      }

      "fail when try to move file from source to desination when semantics == failIfMissing & dst does not exist" in {
        // given
        val src = tempFile(maybeContent = Some("src content"))
        val dst = tempFile(createFile = false)

        exists(src) must_== true
        exists(dst) must_== false

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must_== -\/((PathErr(
          InvalidPath(src, "Can not move to destination that does not exists if semnatics == failIfMissing")))
        )
        exists(src) must_== true
        ok
      }

      "move directory from source to desination when semantics == overwrite & dst does not exist" in {
        // given
        val src = tempDir(withFiles = List(
          "temp1.tmp",
          "temp2.tmp"
        ))
        val dst = tempDir(createDir = false)
        exists(src) must_== true
        exists(dst) must_== false

        getChildren(src) must_== List(
          src </> file("temp1.tmp"),
          src </> file("temp2.tmp")
        )

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        exists(src) must_== false
        exists(dst) must_== true

        getChildren(dst) must_== List(
          dst </> file("temp1.tmp"),
          dst </> file("temp2.tmp")
        )
        ok
      }

      "move directory from source to desination when semantics == overwrite & dst does exist" in {
        // given
        val src = tempDir(withFiles = List(
          "temp1.tmp",
          "temp2.tmp"
        ))
        val dst = tempDir(withFiles = List(
          "temp3.tmp",
          "temp4.tmp"
        ))

        exists(src) must_== true
        exists(dst) must_== true

        getChildren(src) must_== List(
          src </> file("temp1.tmp"),
          src </> file("temp2.tmp")
        )
        getChildren(dst) must_== List(
          dst </> file("temp3.tmp"),
          dst </> file("temp4.tmp")
        )

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        exists(src) must_== false
        exists(dst) must_== true

        getChildren(dst) must_== List(
          dst </> file("temp1.tmp"),
          dst </> file("temp2.tmp")
        )
        ok
      }

      "move directory from source to desination when semantics == failIfExists & dst does not exist" in {
        // given
        val src = tempDir(withFiles = List(
          "temp1.tmp",
          "temp2.tmp"
        ))
        val dst = tempDir(createDir = false)
        exists(src) must_== true
        exists(dst) must_== false

        getChildren(src) must_== List(
          src </> file("temp1.tmp"),
          src </> file("temp2.tmp")
        )

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        exists(src) must_== false
        exists(dst) must_== true

        getChildren(dst) must_== List(
          dst </> file("temp1.tmp"),
          dst </> file("temp2.tmp")
        )
      }

      "fail when try to move dir from source to desination when semantics == failIfExists & dst does exist" in {
        // given
        val src = tempDir(withFiles = List(
          "temp1.tmp",
          "temp2.tmp"
        ))
        val dst = tempDir(withFiles = List(
          "temp3.tmp",
          "temp4.tmp"
        ))
        exists(src) must_== true
        exists(dst) must_== true

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must_== -\/((PathErr(
          InvalidPath(dst, "Can not move to destination that already exists if semnatics == failIfExists")))
        )
      }

      "fail when try to move dir from src to dest when semantics == failIfMissing & dst does not exist" in {
        // given
        val src = tempDir(withFiles = List(
          "temp1.tmp",
          "temp2.tmp"
        ))
        val dst = tempDir(createDir = false)
        exists(src) must_== true
        exists(dst) must_== false

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        result must_== -\/((PathErr(
          InvalidPath(dst, "Can not move to destination that does not exists if semnatics == failIfMissing")))
        )
      }

      "move directory from source to desination when semantics == failIfMissing & dst does exist" in {
        // given
        val src = tempDir(withFiles = List(
          "temp1.tmp",
          "temp2.tmp"
        ))
        val dst = tempDir(withFiles = List(
          "temp3.tmp",
          "temp4.tmp"
        ))
        exists(src) must_== true
        exists(dst) must_== true

        getChildren(src) must_== List(
          src </> file("temp1.tmp"),
          src </> file("temp2.tmp")
        )
        getChildren(dst) must_== List(
          dst </> file("temp3.tmp"),
          dst </> file("temp4.tmp")
        )

        val program = define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // when
        val result = program.run.foldMap(interpreter).unsafePerformSync
        // then
        exists(src) must_== false
        exists(dst) must_== true

        getChildren(dst) must_== List(
          dst </> file("temp1.tmp"),
          dst </> file("temp2.tmp")
        )
      }
    }
  }

  private def interpreter: ManageFile ~> Task =
    local.managefile.interpret[Eff] andThen foldMapNT(NaturalTransformation.refl[Task])

  private def exists(path: APath): Boolean =
    new File(posixCodec.unsafePrintPath(path)).exists()

  private def tempFile(createFile: Boolean = true, maybeContent: Option[String] = None): AFile = {
    val pathStr = s"""${System.getProperty("java.io.tmpdir")}/${scala.util.Random.nextInt().toString}.tmp"""
    val extensin = ".tmp"

    if(createFile) {
      val file = new File(pathStr)
      file.createNewFile()

      maybeContent.foreach { content =>
        val pw = new PrintWriter(file)
        pw.write(content)
        pw.flush
        pw.close
      }
    }

    sandboxAbs(posixCodec.parseAbsFile(pathStr).get)
  }

  private def tempDir(createDir: Boolean = true, withFiles: List[String] = List()): ADir = {
    val root = Paths.get(System.getProperty("java.io.tmpdir"))
    val prefix = "tempDir"
    val dirName = prefix + scala.util.Random.nextInt().toString
    val path = s"$root/$dirName/"

    if(createDir) {
      Files.createDirectory(Paths.get(path))
      withFiles.foreach { fileName =>
        new File(path + fileName).createNewFile()
      }
    }
    sandboxAbs(posixCodec.parseAbsDir(path).get)
  }

  private def getChildren(dir: ADir): List[AFile] = {
    val dirFile = new File(posixCodec.unsafePrintPath(dir))
    dirFile.listFiles.toList.map(f => sandboxAbs(posixCodec.parseAbsFile(f.getAbsolutePath).get))
  }

  private def getContent(f: AFile): List[String] =
    scala.io.Source.fromFile(posixCodec.unsafePrintPath(f)).getLines.toList

  private def define[C]
    (defined: ManageFile.Ops[ManageFile] => FileSystemErrT[Free[ManageFile, ?], C])
    (implicit ops: ManageFile.Ops[ManageFile])
      : FileSystemErrT[Free[ManageFile, ?], C] =
    defined(ops)

}
