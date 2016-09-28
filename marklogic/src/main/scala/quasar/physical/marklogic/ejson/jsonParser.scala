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

package quasar.physical.marklogic.ejson

import quasar.Predef._
import quasar.{ejson => ejs}

import jawn._
import matryoshka._
import scalaz._, Scalaz._

object jsonParser {
  def apply[T[_[_]]: Corecursive, F[_]: Functor](implicit C: ejs.Common :<: F, E: ejs.Extension :<: F): SupportParser[T[F]] =
    new SupportParser[T[F]] {
      implicit val facade: Facade[T[F]] =
        new SimpleFacade[T[F]] {
          def jarray(arr: List[T[F]])         = C(ejs.Arr(arr)).embed
          // TODO: Should `ListMap` really be in the interface, or just used as impl?
          def jobject(obj: Map[String, T[F]]) = E(ejs.Map(obj.toList.map(_.leftMap(k => C(ejs.Str[T[F]](k)).embed)))).embed
          def jnull()                         = C(ejs.Null[T[F]]()).embed
          def jfalse()                        = C(ejs.Bool[T[F]](false)).embed
          def jtrue()                         = C(ejs.Bool[T[F]](true)).embed
          def jnum(n: String)                 = C(ejs.Dec[T[F]](BigDecimal(n))).embed
          def jint(n: String)                 = E(ejs.Int[T[F]](BigInt(n))).embed
          def jstring(s: String)              = C(ejs.Str[T[F]](s)).embed
        }
    }
}
