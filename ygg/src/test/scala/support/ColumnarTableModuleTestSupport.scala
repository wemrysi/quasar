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

package ygg.tests

import ygg._, common._,json._, table._
import scalaz._, Scalaz._

trait ColumnarTableModuleTestSupport extends ColumnarTableModule {
  def toJson(dataset: Table): Need[Stream[JValue]]                         = dataset.toJson.map(_.toStream)
  def toJsonSeq(table: Table): Seq[JValue]                                 = toJson(table).copoint
  def fromSample(sampleData: SampleData): Table                            = fromJson(sampleData.data, None)
  def fromSample(sampleData: SampleData, maxBlockSize: Option[Int]): Table = fromJson(sampleData.data, maxBlockSize)
}
