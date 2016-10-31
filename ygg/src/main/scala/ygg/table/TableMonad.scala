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

package ygg.table

import ygg._, common._
import trans._

// json._
// import quasar._
// import quasar.ejson.EJson
import trans.{ TransSpec1 => Unary }
import trans.{ TransSpec2 => Binary }
import scalaz.{ Source => _, _ }
import scalaz.Scalaz._

trait TableMonad[T <: ygg.table.Table] {
  def transform(table: T)(unary: Unary): T
  // def align(left: T, right: T)(lspec: Unary, rspec: Unary): T -> T
  def cross(left: T, right: T)(crosser: Binary): T
  def cogroup(left: T, right: T)(lkey: Unary, rkey: Unary, ltrans: Unary, rtrans: Unary, btrans: Binary): T

  def sort[F[_]: Monad](table: T)(key: Unary): F[T]
  def groupByN[F[_]: Monad](table: T, keys: Seq[Unary], values: Unary): F[Seq[T]]
}

trait TableMonad0 {
  implicit lazy val columnarTableMonad: TableMonad[ColumnarTable.Table] = new TableMonad.ColumnarTableMonad
}
object TableMonad extends TableMonad0 {
  implicit lazy val blockTableMonad: TableMonad[BlockTable.Table] = new BlockTableMonad

  class ColumnarTableMonad extends TableMonad[ColumnarTable.Table] {
    private type T = ColumnarTable.Table

    def sort[F[_]: Monad](table: T)(key: Unary): F[T]                               = table.point[F]
    // def align(left: T, right: T)(lspec: Unary, rspec: Unary): T -> T                = ???
    def groupByN[F[_]: Monad](table: T, keys: Seq[Unary], values: Unary): F[Seq[T]] = ???

    def transform(table: T)(unary: Unary): T =
      table transform unary

    def cogroup(left: T, right: T)(lkey: Unary, rkey: Unary, ltrans: Unary, rtrans: Unary, btrans: Binary): T =
      left.cogroup(lkey, rkey, right)(ltrans, rtrans, btrans)

    def cross(left: T, right: T)(crosser: Binary): T = (left cross right)(crosser)
  }
  class BlockTableMonad extends TableMonad[BlockTable.Table] {
    import BlockTable._
    private type T = BlockTable.Table
    private def sortOrder = SortAscending

    def sort[F[_]: Monad](table: T)(key: Unary): F[T] = table match {
      case _: SingletonTable => table.point[F]
      case _: InternalTable  => sort[F](table.toExternalTable)(key)
      case _: ExternalTable  => groupByN[F](table, Seq(key), root) map (_.headOption getOrElse Table.empty)
    }

    def groupByN[F[_]: Monad](table: T, keys: Seq[Unary], values: Unary): F[Seq[T]] = {
      ().point[F] map (_ =>
        table.groupByN(keys, values, SortAscending, unique = false).value.toVector
      )
    }

    def transform(table: T)(unary: Unary): T =
      table transform unary

    def cogroup(left: T, right: T)(lkey: Unary, rkey: Unary, ltrans: Unary, rtrans: Unary, btrans: Binary): T =
      left.cogroup(lkey, rkey, right)(ltrans, rtrans, btrans)

    def cross(left: T, right: T)(crosser: Binary): T = (left cross right)(crosser)
  }
}
