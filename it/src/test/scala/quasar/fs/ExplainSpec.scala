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

package quasar.fs

import quasar.Predef._

import pathy.Path._
import scalaz.syntax.functor._
import scalaz.syntax.show._

class ExplainSpec
  extends FileSystemTest[FileSystem](FileSystemTest.allFsUT)
  with quasar.CompilerHelpers {

  import FileSystemTest.oneDoc

  val query  = QueryFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val dataFile: AFile = rootDir </> dir("explain") </> file("data")
  val dataStr: String = posixCodec.printPath(dataFile)

  def explainQuery(sql: String, fs: FileSystemUT[FileSystem]) =
    s"QUERY: $sql" >> {
      val result =
        query
          .explain(fullCompileExp(sql))
          .run.run

      fs.testInterpM(result).map { case (phases, disj) =>
        disj.fold(
          err => ko(err.shows).toResult,
          _   => pending(phases.mkString("\n\n")))
      }.unsafePerformSync
    }

  fileSystemShould { fs =>
    step(runT(fs.setupInterpM)(write.saveThese(dataFile, oneDoc).void).runVoid)

    "Explain Queries" should {

      explainQuery(s"select * from `$dataStr`", fs)

      explainQuery(s"select * from `$dataStr` limit 10", fs)

    }

    step(runT(fs.setupInterpM)(manage.delete(dataFile)).runVoid)
  }
}
