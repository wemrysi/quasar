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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.Data
import quasar.SKI.κ
import quasar.ejson.EJson
import quasar.fp.{interpret, interpretM}
import quasar.physical.marklogic.MonadErrMsg
import quasar.physical.marklogic.xml._

import scala.xml._

import jawn._
import matryoshka._
import matryoshka.patterns._
import scalaz.{Bitraverse, Traverse, Id}
import scalaz.syntax.monadError._

object data {
  object JsonParser extends SupportParser[Data] {
    implicit val facade: Facade[Data] =
      new SimpleFacade[Data] {
        def jarray(arr: List[Data]) = Data.Arr(arr)
        // TODO: Should `ListMap` really be in the interface, or just used as impl?
        def jobject(obj: Map[String, Data]) = Data.Obj(ListMap(obj.toList: _*))
        def jnull() = Data.Null
        def jfalse() = Data.False
        def jtrue() = Data.True
        def jnum(n: String) = Data.Dec(BigDecimal(n))
        def jint(n: String) = Data.Int(BigInt(n))
        def jstring(s: String) = Data.Str(s)
      }
  }

  def encodeXml[F[_]: MonadErrMsg](data: Data): F[Node] =
    data.hyloM[F, CoEnv[Data, EJson, ?], Node](
      interpretM(
        d => s"No representation for '$d' in XML.".raiseError[F, Node],
        EncodeXml[F, EJson].encodeXml),
      Data.toEJson[EJson] andThen (_.point[F]))

  def decodeXml(node: Node): Data =
    node.hylo[CoEnv[Node, EJson, ?], Data](
      interpret(κ(Data.NA), Data.fromEJson),
      DecodeXml[Id.Id, EJson].decodeXml andThen (CoEnv(_)))

  // TODO{matryoshka}: Remove once we've upgraded to 0.11.2+
  private implicit def coenvTraverse[E]: Traverse[CoEnv[E, EJson, ?]] =
    Bitraverse[CoEnv[?, EJson, ?]].rightTraverse
}
