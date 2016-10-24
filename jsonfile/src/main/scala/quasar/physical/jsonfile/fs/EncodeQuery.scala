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

package quasar.physical.jsonfile.fs

import quasar._, Predef._
import Planner.Rep
import matryoshka._
import scalaz._, Scalaz._

trait EncodeQuery[M[_], F[_]] {
  def encodeQuery: AlgebraM[M, F, Rep]
}

object EncodeQuery {
  def apply[M[_], F[_]](implicit z: EncodeQuery[M, F]): EncodeQuery[M, F] = z

  def make[M[_], F[_]](f: F[Rep] => M[Rep]): EncodeQuery[M, F] = new EncodeQuery[M, F] { val encodeQuery = f }

  implicit def coproductEncodeQuery[M[_], F[_], G[_]](implicit F: EncodeQuery[M, F], G: EncodeQuery[M, G]): EncodeQuery[M, Coproduct[F, G, ?]] =
    make(_.run.fold(F.encodeQuery, G.encodeQuery))

  implicit def commonEncodeQuery[M[_]]: EncodeQuery[M, ejson.Common] = make {
    case ejson.Arr(xs) => ???
    case ejson.Null()  => ???
    case ejson.Bool(b) => ???
    case ejson.Str(s)  => ???
    case ejson.Dec(d)  => ???
  }
  implicit def extensionEncodeQuery[M[_]]: EncodeQuery[M, ejson.Extension] = make {
    case ejson.Meta(value, meta) => ???
    case ejson.Map(entries)      => ???
    case ejson.Byte(b)           => ???
    case ejson.Char(c)           => ???
    case ejson.Int(i)            => ???
  }
}
