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
import scalaz.{ Source => _, _ }, Scalaz._
import trans.{ TransSpec1 => Unary }
import trans.{ TransSpec2 => Binary }

trait TableMonad[T <: ygg.table.Table] {
  def sort[F[_]: Monad](table: T)(key: Unary): F[T]
  def transform[F[_]: Monad](table: T)(unary: Unary): F[T]

  def align[F[_]: Monad](left: T, right: T)(lspec: Unary, rspec: Unary): F[T -> T]
  def cross[F[_]: Monad](left: T, right: T)(crosser: Binary): F[T]
  def join[F[_]: Monad](left: T, right: T)(lspec: Unary, rspec: Unary, joiner: Binary): F[T]
  def cogroup[F[_]: Monad](left: T, right: T)(lkey: Unary, rkey: Unary, ltrans: Unary, rtrans: Unary, btrans: Binary): F[T]
}

object TableMonad {
  class ColumnarOp extends TableMonad[ColumnarTable.Table] {
    private type T = ColumnarTable.Table

    def sort[F[_]: Monad](table: T)(key: Unary): F[T]                                                                         = ???
    def transform[F[_]: Monad](table: T)(unary: Unary): F[T]                                                                  = ???
    def align[F[_]: Monad](left: T, right: T)(lspec: Unary, rspec: Unary): F[T -> T]                                          = ???

    def cogroup[F[_]: Monad](left: T, right: T)(lkey: Unary, rkey: Unary, ltrans: Unary, rtrans: Unary, btrans: Binary): F[T] =
      left.cogroup(lkey, rkey, right)(ltrans, rtrans, btrans).point[F]

    def join[F[_]: Monad](left: T, right: T)(lkey: Unary, rkey: Unary, joiner: Binary): F[T] = {
      for {
        l  <- sort[F](left)(lkey)
        r  <- sort[F](right)(rkey)
        co <- cogroup[F](l, r)(lkey, rkey, root.emptyArray, root.emptyArray, joiner.wrapArrayValue)
        r  <- transform[F](co)(root(0))
      }
      yield r
    }
    def cross[F[_]: Monad](left: T, right: T)(spec: Binary): F[T] = (left.point[F] |@| right.point[F])((l, r) => (l cross r)(spec))
  }
}
