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

package ygg

import quasar._, Predef._
import quasar.ejson.EJson
import jawn.SimpleFacade

package object macros {
  implicit class SimpleFacadeOps[J](facade: SimpleFacade[J]) {
    def xmap[K](f: J => K, g: K => J): SimpleFacade[K] = new SimpleFacade[K] {
      def jtrue(): K                     = f(facade.jtrue())
      def jfalse(): K                    = f(facade.jfalse())
      def jnull(): K                     = f(facade.jnull())
      def jint(s: String): K             = f(facade.jint(s))
      def jnum(s: String): K             = f(facade.jnum(s))
      def jstring(s: String): K          = f(facade.jstring(s))
      def jarray(vs: scala.List[K]): K   = f(facade.jarray(vs map g))
      def jobject(vs: Map[String, K]): K = f(facade.jobject(vs mapValues g))
    }
  }
  implicit class QuasarTryOps[A](private val self: Try[A]) extends AnyVal {
    def |(expr: => A): A = fold(_ => expr, x => x)
    def fold[B](f: Throwable => B, g: A => B): B = self match {
      case scala.util.Success(x) => g(x)
      case scala.util.Failure(t) => f(t)
    }
  }
  implicit object DataFacade extends SimpleFacade[Data] {
    def jtrue(): Data                        = Data.True
    def jfalse(): Data                       = Data.False
    def jnull(): Data                        = Data.Null
    def jint(s: String): Data                = Data.Int(BigInt(s))
    def jnum(s: String): Data                = Data.Dec(BigDecimal(s))
    def jstring(s: String): Data             = Data.Str(s)
    def jarray(vs: List[Data]): Data         = Data.Arr(vs)
    def jobject(vs: Map[String, Data]): Data = Data.Obj((ListMap.newBuilder[String, Data] ++= vs).result)
  }

  implicit def liftData(x: Data): EJson[Data] =
    Data.toEJson[EJson].apply(x).run.fold[EJson[Data]](x => x, x => x)

  implicit lazy val EJsonDataFacade: SimpleFacade[EJson[Data]] =
    DataFacade.xmap[EJson[Data]](liftData, Data.fromEJson)
}
