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
import quasar.QuasarSpecification
import quasar.fp.free._
import quasar.fs.PathError._
import quasar.fs.FileSystemError._
import quasar.fs._
import quasar.fs.ManageFile.MoveSemantics
import quasar.effect.Failure

import org.specs2.ScalaCheck
import org.specs2.scalaz._
import pathy.Path._
import scalaz.{Failure => _, _}
import Scalaz._
import scalaz.concurrent.Task

class ManageFileSpec extends QuasarSpecification with ScalaCheck with DisjunctionMatchers
    with TempFSSugars {

  type Eff[A] = Coproduct[Task, PhysErr, A]

  "managefile" should {
    "delete" should {

      "delete file on a local FS if exists" in {
        // given
         val program = (path: AFile) => define { unsafe =>
          for {
            _ <- unsafe.delete(path)
          } yield ()
         }
        // when
        withTempFile(createIt = withNoContent) { path =>
          for {
            existed <- exists(path)
            result <- execute(program(path))
            existsAfter <- exists(path)
          } yield {
            // then
            result  must_= \/-(())
            existed must beTrue
            existsAfter must beFalse
          }
        }
      }

      "fail to delete if file does NOT exist" in {
                // given
         val program = (path: AFile) => define { unsafe =>
          for {
            _ <- unsafe.delete(path)
          } yield ()
         }
        // when
        withTempFile(createIt = None) { path =>
          for {
            existed <- exists(path)
            result <- execute(program(path))
          } yield {
            // then
            existed must beFalse
            result must_= -\/((PathErr(PathNotFound(path))))
          }
        }
      }
    }

    "tempFile" should {
      "create temp file near non existing dir path" in {
        // given
        val program = (nearDir: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearDir)
          } yield (filePath)
        }
        // when
        withTempDir(createIt = false, withTailDir = List("foo", "bar")) { nearDir =>
          for {
            result <- execute(program(nearDir))
          } yield {
            // then
            result must beRightDisjunction.like {
              case path =>
                posixCodec.unsafePrintPath(path)
                  .startsWith(posixCodec.unsafePrintPath(nearDir)) must beTrue
            }
          }
        }
      }

      "create temp file near existing dir path" in {
        // given
        val program = (nearDir: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearDir)
          } yield (filePath)
        }
        // when
        withTempDir(createIt = true) { nearDir =>
          for {
            result <- execute(program(nearDir))
          } yield {
            // then
            result must beRightDisjunction.like {
              case path =>
                posixCodec.unsafePrintPath(path)
                  .startsWith(posixCodec.unsafePrintPath(nearDir)) must beTrue
            }
          }
        }
      }

      "create temp file near existing file path" in {
        // given
        val program = (nearFile: APath) => define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearFile)
          } yield (filePath)
        }
        // when
        withTempFile(createIt = None) { nearFile =>
          for {
            result <- execute(program(nearFile))
          } yield {
            // then
            result must beRightDisjunction.like {
              case path =>
                posixCodec.unsafePrintPath(path)
                  .startsWith(posixCodec.unsafePrintPath(parentDir(nearFile).get)) must beTrue
            }

          }
        }
      }      
    }

    "move" should {

      "move file from source to desination when semantics == overwrite & dst does not exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("some content")), withTailDir = List("foo")) { src =>
          Task.delay {
            withTempFile(createIt = None, withTailDir = List("bar")) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                dstContent <- getContent(dst)
              } yield {
                // then
                srcExists must_== false
                dstExists must_== true
                dstContent must_= List("some content")
              }
            }
          }
        }
      }

      "move file from source to desination when semantics == overwrite & dst does exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("src content"))) { src =>
          Task.delay {
            withTempFile(createIt = Some(List("dst content"))) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                dstContent <- getContent(dst)
              } yield {
                // then
                srcExists must beFalse
                dstExists must beTrue
                dstContent must_= List("src content")
              }
            }
          }
        }
      }

      "move file from src to dst when semantics == failIfExists & dst does not exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("some content"))) { src =>
          Task.delay {
            withTempFile(createIt = None) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                dstContent <- getContent(dst)
              } yield {
                // then
                srcExists must beFalse
                dstExists must beTrue
                dstContent must_= List("some content")
              }
            }
          }
        }
      }

      "fail when try to move file from src to dst when semantics == failIfExists & dst does exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("src content"))) { src =>
          Task.delay {
            withTempFile(createIt = Some(List("dst content"))) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                srcContent <- getContent(src)
                dstContent <- getContent(dst)
              } yield {
                // then
                result must_= -\/((PathErr(pathExists(dst))))
                srcExists must beTrue
                dstExists must beTrue
                srcContent must_= List("src content")
                dstContent must_= List("dst content")
              }
            }
          }
        }
      }

      "move file from src to dst when semantics == failIfMissing & dst does exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("src content"))) { src =>
          Task.delay {
            withTempFile(createIt = Some(List("dst content"))) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                dstContent <- getContent(dst)
              } yield {
                // then
                srcExists must beFalse
                dstExists must beTrue
                dstContent must_= List("src content")
              }
            }
          }
        }
      }

      "fail when try to move file from src to dst when semantics == failIfMissing & dst does not exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("src content"))) { src =>
          Task.delay {
            withTempFile(createIt = None) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)

              } yield {
                // then
                result must_= -\/((PathErr(pathNotFound(dst))))
                srcExists must beTrue
              }
            }
          }
        }
      }

      "move directory from src to dst when semantics == overwrite & dst does not exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = false) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                children <- getChildren(dst)
              } yield {
                srcExists must beFalse
                dstExists must beTrue
                children must_= List(
                  dst </> file("temp1.tmp"),
                  dst </> file("temp2.tmp")
                )
              }
            }
          }
        }
      }

      "move directory from src to dst when semantics == overwrite & dst does exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = true) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                _ <- createFile(dst, "temp3.tmp")
                _ <- createFile(dst, "temp4.tmp")
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                children <- getChildren(dst)
              } yield {
                srcExists must beFalse
                dstExists must beTrue
                children must_= List(
                  dst </> file("temp1.tmp"),
                  dst </> file("temp2.tmp")
                )
              }
            }
          }
        }
      }

      "move directory from src to dst when semantics == failIfExists & dst does not exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = false) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                children <- getChildren(dst)
              } yield {
                srcExists must beFalse
                dstExists must beTrue
                children must_= List(
                  dst </> file("temp1.tmp"),
                  dst </> file("temp2.tmp")
                )
              }
            }
          }
        }
      }

      "fail when try to move dir from src to dst when semantics == failIfExists & dst does exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // when
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = true) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                _ <- createFile(dst, "temp3.tmp")
                _ <- createFile(dst, "temp4.tmp")
                result <- execute(program(src, dst))
              } yield {
                // then
                result must_= -\/((PathErr(pathExists(dst)))
                )
              }
            }
          }
        }

      }

      "fail when try to move dir from src to dest when semantics == failIfMissing & dst does not exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = false) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                result <- execute(program(src, dst))
              } yield {
                // then
                result must_= -\/((PathErr(pathNotFound(dst)))
                )
              }
            }
          }
        }
      }

      "move directory from src to dst when semantics == failIfMissing & dst does exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // when
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = true) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                _ <- createFile(dst, "temp3.tmp")
                _ <- createFile(dst, "temp4.tmp")
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                children <- getChildren(dst)
              } yield {
                // then
                srcExists must beFalse
                dstExists must beTrue
                children must_= List(
                  dst </> file("temp1.tmp"),
                  dst </> file("temp2.tmp")
                )
              }
            }
          }
        }
      }
    }
  }

  private def execute[C](program: FileSystemErrT[Free[ManageFile, ?], C]):
      Task[FileSystemError \/ C] =
    program.run.foldMap(interpreter)

  private def interpreter: ManageFile ~> Task = {

    import Failure.Fail

    val errorInter: PhysErr ~> Task =
      new (PhysErr ~> Task) {
        def apply[A](perr: PhysErr[A]): Task[A] = perr match {
          case Fail(er) => Task.delay {
            throw er.cause
          }
        }
      }

    def inner: Eff ~> Task =
      NaturalTransformation.refl[Task] :+: errorInter

    local.managefile.interpret[Eff] andThen foldMapNT(inner)
  }

  private def define[C]
    (defined: ManageFile.Ops[ManageFile] => FileSystemErrT[Free[ManageFile, ?], C])
    (implicit ops: ManageFile.Ops[ManageFile])
      : FileSystemErrT[Free[ManageFile, ?], C] =
    defined(ops)

}
