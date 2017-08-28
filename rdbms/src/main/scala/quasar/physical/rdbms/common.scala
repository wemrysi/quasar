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
import slamdata.Predef._
import doobie.imports.IOLite
import doobie.util.transactor.Transactor
import pathy.Path
import quasar.contrib.pathy.AFile

object common {

  final case class Config(transactor: Transactor[IOLite])

  final case class SchemaName(name: String) extends AnyVal
  final case class TableName(name: String) extends AnyVal
  final case class TablePath(schema: Option[SchemaName], table: TableName)

  object TablePath {
    def create(file: AFile): TablePath = {
      val filename = Path.fileName(file).value
      val dirname = Path.parentDir(file).flatMap(Path.dirName).map(_.value)
      new TablePath(dirname.map(SchemaName.apply), TableName(filename))
    }
  }
}
