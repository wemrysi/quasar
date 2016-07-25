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
import quasar.Data
import quasar.fp.TaskRef
import quasar.fp.numeric._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.WriteFile.WriteHandle
import quasar.fs.FileSystemError._
import quasar.effect._

import java.lang.System
import java.io._
import java.nio.file._

import org.specs2.ScalaCheck
import pathy.Path.posixCodec
import pathy.Path._
import scalaz._, Scalaz._, concurrent.Task

class WriteFileSpec extends QuasarSpecification with ScalaCheck  {

  type Eff0[A] = Coproduct[KeyValueStore[WriteHandle, PrintWriter, ?], Task, A]
  type Eff[A] = Coproduct[MonotonicSeq, Eff0, A]

  "writefile" should {


    "open -> write chunks -> close -> assert file with results" in {
      // given
      val chunks = (user("john", 31) :: user("anna", 21) :: Nil).toVector
      val program = (path: AFile) => define { writeUnsafe =>
        for {
          wh <- writeUnsafe.open(path)
          errors <- writeUnsafe.write(wh, chunks).liftM[FileSystemErrT]
          _ <- writeUnsafe.close(wh).liftM[FileSystemErrT]
        } yield (errors)
      }
      // when
      withTempFile { path =>
        for {
          result <- execute[Vector[FileSystemError]](program(path))
          content <- getContent(path)
        } yield{
          // then
          result must_= \/-(Vector.empty[FileSystemError])
          content  must_= List(
            """{ "login": "john", "age": 31 }""",
            """{ "login": "anna", "age": 21 }"""
          )
        }
      }
    }

    "(open -> write chunks -> close) -> (open -> write chunks -> close) -> assert file with results" in {
      // given
      val chunks1 = (user("john", 31) :: user("anna", 21) :: Nil).toVector
      val chunks2 = (user("kate", 22) :: Nil).toVector

      val program = (path: AFile) => define { writeUnsafe =>
        for {
          wh1 <- writeUnsafe.open(path)
          errors1 <- writeUnsafe.write(wh1, chunks1).liftM[FileSystemErrT]
          _ <- writeUnsafe.close(wh1).liftM[FileSystemErrT]
          wh2 <- writeUnsafe.open(path)
          errors2 <- writeUnsafe.write(wh2, chunks2).liftM[FileSystemErrT]
          _ <- writeUnsafe.close(wh2).liftM[FileSystemErrT]

        } yield (errors1 ++ errors2)
      }
      // when
      withTempFile { path =>
        for {
          result <- execute[Vector[FileSystemError]](program(path))
          content <- getContent(path)
        } yield {
          // then
          result must_= \/-(Vector.empty[FileSystemError])
          content must_= List(
            """{ "login": "john", "age": 31 }""",
            """{ "login": "anna", "age": 21 }""",
            """{ "login": "kate", "age": 22 }"""
          )
        }
      }
    }
  }

  private def execute[C](program: FileSystemErrT[Free[WriteFile, ?], C]):
      Task[FileSystemError \/ C] = interpreter.flatMap(program.run.foldMap(_))
 
  private def interpreter: Task[WriteFile ~> Task] = {

    def innerInterpreter: Task[Eff ~> Task] =  {
      (TaskRef(0L) |@| TaskRef(Map.empty[WriteHandle, PrintWriter])) { (genState, kvsState) =>
        MonotonicSeq.fromTaskRef(genState) :+:
        KeyValueStore.fromTaskRef[WriteHandle, PrintWriter](kvsState) :+:
        NaturalTransformation.refl[Task]
      }
    }

    innerInterpreter.map { inner =>
      local.writefile.interpret[Eff] andThen foldMapNT[Eff, Task](inner)
    }
  }

  private def withTempFile[C](run: AFile => Task[C]): C = {

    def genTempFilePath: Task[AFile] = Task.delay {
      val path = System.getProperty("java.io.tmpdir") +
      "/" + scala.util.Random.nextInt().toString + ".tmp"
      sandboxAbs(posixCodec.parseAbsFile(path).get)
    }

    def deleteFile(file: AFile): Task[Unit] = Task.delay {
      Files.delete(Paths.get(posixCodec.unsafePrintPath(file)))
    }

    val execution: Task[C] = for {
      filePath <- genTempFilePath
      result <- run(filePath).onFinish {
        _ => deleteFile(filePath)
      }
    } yield result

    execution.unsafePerformSync
  }

  private def define[C]
    (defined: WriteFile.Unsafe[WriteFile] => FileSystemErrT[Free[WriteFile, ?], C])
    (implicit writeUnsafe: WriteFile.Unsafe[WriteFile])
      : FileSystemErrT[Free[WriteFile, ?], C] =
    defined(writeUnsafe)
  
  private def user(login: String, age: Int) =
    Data.Obj(ListMap("login" -> Data.Str(login), "age" -> Data.Int(age)))

  private def getContent(f: AFile): Task[List[String]] =
    Task.delay(scala.io.Source.fromFile(posixCodec.unsafePrintPath(f)).getLines.toList)

}
