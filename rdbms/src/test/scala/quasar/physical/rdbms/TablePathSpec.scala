/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.rdbms

import pathy.Path
import Path.{FileName, dir1, _}
import quasar.physical.rdbms.common.{SchemaName, TableName, TablePath}
import quasar.Qspec
import quasar.contrib.pathy.AFile
import quasar.contrib.pathy._
import pathy.scalacheck.PathyArbitrary._

class TablePathSpec extends Qspec {

  "TablePath" should {

    "extract table name" in {
      prop {
        (file: AFile) =>
          TablePath.create(file).table must_=== TableName(Path.fileName(file).value)
      }
    }

    "extract empty schema name" in {
      prop {
        (fileName: FileName) => {
          TablePath.create(rootDir </> file1(fileName)).schema must beNone
        }
      }
    }

    "extract some schema name" in {
      prop {
        (parentDirName: DirName, fileName: FileName) => {
          val file = rootDir </> dir1(parentDirName) </> file1(fileName)
          TablePath.create(file).schema must beSome(SchemaName(parentDirName.value))
        }
      }
    }

    "extract some schema name for multiple levels of depth" in {
      prop {
        (dirName1: DirName, dirName2: DirName, fileName: FileName) => {
          val file = rootDir </> dir1(dirName1) </> dir1(dirName2) </> file1(fileName)
          TablePath.create(file).schema must beSome(SchemaName(dirName2.value))
        }
      }
    }
  }

}
