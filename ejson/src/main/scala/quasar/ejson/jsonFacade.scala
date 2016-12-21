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

package quasar.ejson

import quasar.Predef._
import jawn._
import matryoshka._, implicits._
import scalaz._

object jawnFacade {
  def apply[A](implicit z: Facade[A]): Facade[A] = z
}

object jsonFacade {
  def apply[T, F[_]: Functor](implicit T: Corecursive.Aux[T, F], C: Json :<: F): Facade[T] = {
    def rc(x: Common[T]): T = C(Coproduct.rightc(x)).embed
    def lc(x: Obj[T]): T    = C(Coproduct.leftc(x)).embed

    new SimpleFacade[T] {
      def jarray(arr: List[T])            = rc(Arr(arr))
      def jobject(obj: sciMap[String, T]) = lc(Obj(ListMap(obj.toList: _*)))
      def jnull()                         = rc(Null())
      def jfalse()                        = rc(Bool(false))
      def jtrue()                         = rc(Bool(true))
      def jnum(n: String)                 = rc(Dec(BigDecimal(n)))
      def jint(n: String)                 = rc(Dec(BigDecimal(n)))
      def jstring(s: String)              = rc(Str(s))
    }
  }
}
