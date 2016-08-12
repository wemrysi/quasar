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
import quasar.fp.TaskRef
import quasar.fp.numeric._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.ReadFile.ReadHandle
import quasar.effect._

import java.io._
import java.nio.file.{Files, Paths}
import java.lang.System

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import pathy.Path.posixCodec
import scalaz._, Scalaz._, concurrent.Task
import org.apache.spark._

class ReadFileSpec extends Specification with ScalaCheck  {

  type Eff0[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], Read[SparkContext, ?], A]
  type Eff1[A] = Coproduct[Task, Eff0, A]
  type Eff[A] = Coproduct[MonotonicSeq, Eff1, A]


  "readfile" should {
    "open - read chunk - close" in {
      // given
      import quasar.Data._
      val content = List(
        """{"login" : "john", "age" : 28}""",
        """{"login" : "kate", "age" : 31}"""
      )
      val program = (f: AFile) => define { unsafe =>
        for {
          handle   <- unsafe.open(f, Natural(0).get, None)
          readData <- unsafe.read(handle)
          _        <- unsafe.close(handle).liftM[FileSystemErrT]
        } yield readData
      }

      // when
      withTempFile(createIt = Some(content)) { aFile =>
        for {
          sc <- newSc()
          result <- execute(program(aFile), sc)
        } yield {
          result must beLike {
            case \/-(results) =>
              // then
              results.size must be_==(2)
              results must contain(Obj(ListMap("login" -> Str("john"), "age" -> Int(28))))
              results must contain(Obj(ListMap("login" -> Str("kate"), "age" -> Int(31))))
          }
          sc.stop()
        }
      }
      ok
    }
  }

  private def define[C]
    (defined: ReadFile.Unsafe[ReadFile] => FileSystemErrT[Free[ReadFile, ?], C])
    (implicit writeUnsafe: ReadFile.Unsafe[ReadFile])
      : FileSystemErrT[Free[ReadFile, ?], C] =
    defined(writeUnsafe)

  private def execute[C](program: FileSystemErrT[Free[ReadFile, ?], C], sc: SparkContext):
      Task[FileSystemError \/ C] = interpreter(sc).flatMap(program.run.foldMap(_))

  private def interpreter(sc: SparkContext): Task[ReadFile ~> Task] = {

    def innerInterpreter: Task[Eff ~> Task] =  {
      (TaskRef(0L) |@| TaskRef(Map.empty[ReadHandle, SparkCursor])) { (genState, kvsState) =>
        MonotonicSeq.fromTaskRef(genState) :+:
        NaturalTransformation.refl[Task] :+:
	KeyValueStore.fromTaskRef[ReadHandle, SparkCursor](kvsState) :+:
        Read.constant[Task, SparkContext](sc)
      }
    }

    innerInterpreter.map { inner =>
      readfile.interpret[Eff](local.readfile.input[Eff]) andThen foldMapNT[Eff, Task](inner)
    }
  }

  private def newSc(): Task[SparkContext] = Task.delay {
    val config = new SparkConf().setMaster("local[*]").setAppName(this.getClass().getName())
    new SparkContext(config)
  }

  type Content = List[String]

  private def withTempFile[C](createIt: Option[Content] = None)(run: AFile => Task[C]): C = {

    def genTempFilePath: Task[AFile] = Task.delay {
      val path = System.getProperty("java.io.tmpdir") +
      "/" + scala.util.Random.nextInt().toString + ".tmp"
      sandboxAbs(posixCodec.parseAbsFile(path).get)
    }

    def createFile(filePath: AFile): Task[Unit] = Task.delay {
      createIt.foreach { content =>
        val file = toNioPath(filePath).toFile()
        val writer = new PrintWriter(file)
        content.foreach {
          line => writer.write(line + "\n")
        }
        writer.flush()
        writer.close()
      }
    }

    def deleteFile(file: AFile): Task[Unit] = Task.delay {
      Files.delete(Paths.get(posixCodec.unsafePrintPath(file)))
    }

    val execution: Task[C] = for {
      filePath <- genTempFilePath
      _ <- createFile(filePath)
      result <- run(filePath).onFinish {
        _ => deleteFile(filePath)
      }
    } yield result

    execution.unsafePerformSync
  }

  private def toNioPath(path: APath) =
    Paths.get(posixCodec.unsafePrintPath(path))



}
