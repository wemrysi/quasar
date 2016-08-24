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

import scalaz.syntax.show._

class ExplainSpec
  extends FileSystemTest[FileSystem](FileSystemTest.allFsUT)
  with quasar.CompilerHelpers {

  def explainQuery(sql: String, fsut: FileSystemUT[FileSystem]) =
    s"QUERY: $sql" >> {
      val result =
        QueryFile.Ops[FileSystem]
          .explain(fullCompileExp(sql))
          .run.run

      fsut.testInterpM(result).map { case (phases, disj) =>
        disj.fold(
          err => pending(err.shows),
          _ => pending(phases.mkString("\n\n")))
      }.unsafePerformSync
    }

  fileSystemShould { fs =>
    "Explain Queries" should {

//    explainQuery("select * from `/foo`", fs)

      explainQuery("select * from `/foo/bar/baz` limit 10", fs)

//    explainQuery("select baz.foo[*] from `/foo/bar/baz`", fs)
    }
  }
}
