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

package quasar.physical.fallback.fs

import quasar.Predef._
import blueeyes.yggConfig
import blueeyes.json.JValue
import com.precog.yggdrasil.vfs.StubVFSMetadata
import com.precog.bytecode.JType
import com.precog.mimir._
import com.precog._, yggdrasil._, vfs._, table._
import com.precog.common._, security._
import scalaz._, Scalaz._

trait PrecogEvaluator[M[+_]] extends StdLibEvaluatorStack[M] with BlockStoreColumnarTableModule[M] {
  type ErrOr[A] = EitherT[M, ResourceError, A]

  def projections: Map[Path, Map[ColumnRef, Long]]

  def fromJson(values: JValue*): Table = fromRValues(values.toStream map RValue.fromJValue)

  object vfs extends StubVFSMetadata[M](projections)

  trait TableCompanion extends BlockStoreColumnarTableCompanion

  object Table extends TableCompanion {
    def load(table: Table, apiKey: APIKey, tpe: JType): ErrOr[Table] = EitherT right table.point[M]
  }

  def sliceSize: Int = yggConfig.maxSliceSize
  def fromRValues(values: Stream[RValue]): Table = {
    def makeSlice(data: Stream[RValue]): Slice -> Stream[RValue] =
      data splitAt sliceSize leftMap (Slice fromRValues _)

    Table(
      StreamT.unfoldM(values)(events => M.point(events.nonEmpty option makeSlice(events.toStream))),
      ExactSize(values.length.toLong)
    )
  }
}
object PrecogEvaluator {
  def apply[M[+_]: Monad](ps: Map[Path, Map[ColumnRef, Long]]): PrecogEvaluator[M] = new PrecogEvaluator[M] {
    val M           = implicitly[Monad[M]]
    val projections = ps
  }
}

object fall extends PrecogEvaluator[Id] {
  val M           = implicitly[Monad[Id]]
  val projections = Map[Path, Map[ColumnRef, Long]]()

  implicit class TableOps(private val self: Table) {
    def to_s: String = self.slices.toStream.copoint.toVector mkString "\n"
  }
}
