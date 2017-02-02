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

package quasar.parquet

import quasar.Predef._
import quasar.Data
import quasar.QuasarSpecification

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader

class DataReadSupportSpec extends QuasarSpecification {

  def readAll(reader: ParquetReader[Data]): List[Data] = {
    val data: Data = reader.read()
    if(data != null) data :: readAll(reader) else Nil
  }

  def read(reader: ParquetReader[Data]): Unit = {
    println(reader.read())
  }

  "DataReadSupport " should {
    "read primitive types" in {
      val path = new Path("parquet/src/test/resources/test-data-1.parquet")
      val readSupport = new DataReadSupport()
      val reader = ParquetReader.builder[Data](readSupport, path).build()
      val data = readAll(reader)
      data must_== List(
        Data.Obj(
          "score" -> Data.Dec(13.9),
          "age" -> Data.Int(11),
          "id" -> Data.Int(1),
          "active" -> Data.Bool(false),
          "height" -> Data.Dec(101.19999694824219)
        ),
         Data.Obj(
          "score" -> Data.Dec(14.9),
          "age" -> Data.Int(12),
          "id" -> Data.Int(2),
          "active" -> Data.Bool(true),
          "height" -> Data.Dec(102.19999694824219)
        )
      )
      ok
    }
  }
}
