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

package quasar.physical.marklogic.xml

import quasar.Predef._
import quasar.{ejson => ejs}

import scala.xml.Node

import matryoshka._
import scalaz.{Node => _, _}

trait DecodeXml[M[_], F[_]] {
  def decodeXml: ElgotCoalgebraM[Node \/ ?, M, F, Node]
}

object DecodeXml extends DecodeXmlInstances {
  def apply[M[_], F[_]](implicit D: DecodeXml[M, F]): DecodeXml[M, F] = D
}

sealed abstract class DecodeXmlInstances {
  implicit def decodeEJson[M[_], F[_]](implicit C: ejs.Common :<: F, E: ejs.Extension :<: F): DecodeXml[M, F] =
    new DecodeXml[M, F] {
      val decodeXml: ElgotCoalgebraM[Node \/ ?, M, F, Node] = _ => ???
    }
}
