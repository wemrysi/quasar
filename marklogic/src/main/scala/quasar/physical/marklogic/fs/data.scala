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
import quasar.physical.marklogic.xquery.xml.QName

import scala.xml._

import jawn._

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

  // TODO: Error type for when data keys aren't valid QNames
  // TODO: Reuse names from xquery.ejson
  // TODO: Should only need ejson:array and quasar:data for top-level objects, inner objects can use property names as element names
  // TODO: Do we need to declare the top-level quasar/ejson namespaces?
  def toXML(elementName: QName): Data => NodeSeq = {
    case Data.Binary(bytes) => ???
    case Data.Bool(b)       => ???
    case Data.Date(d)       => ???
    case Data.Dec(d)        => ???
    case Data.Id(s)         => ???
    case Data.Int(i)        => ???
    case Data.Interval(ivl) => ???
    case Data.Null          => ???
    case Data.Str(s)        => ???
    case Data.Time(t)       => ???
    case Data.Timestamp(ts) => ???

    case Data.Arr(xs)       => ???
    case Data.Obj(xs)       => ???

    // TODO: These can only appear in LogicalPlan, no?
    case Data.Set(_)        => NodeSeq.Empty
    case Data.NA            => NodeSeq.Empty
  }
}
