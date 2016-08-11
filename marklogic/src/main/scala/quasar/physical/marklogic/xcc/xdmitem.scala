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

package quasar.physical.marklogic.xcc

import quasar.Predef._
import quasar.Data

import com.marklogic.xcc.types._
import scalaz._

object xdmitem {
  // TODO: What sort of error handling to we want?
  val toData: XdmItem => Option[Data] = {
    case item: CtsBox                   => None
    case item: CtsCircle                => None
    case item: CtsPoint                 => None
    case item: CtsPolygon               => None
    // TODO: What is the difference between JSArray/Object and their *Node variants?
    case item: JSArray                  => None
    case item: JSObject                 => None
    case item: JsonItem                 => data.JsonParser.parseFromString(item.asString).toOption
    case item: XdmAttribute             => None
    // TODO: Inefficient for large data as it must be buffered into memory
    case item: XdmBinary                => Some(Data.Binary(ImmutableArray.fromArray(item.asBinaryData)))
    case item: XdmComment               => None
    case item: XdmDocument              => None
    case item: XdmElement               => None
    case item: XdmProcessingInstruction => None
    case item: XdmText                  => None
    case item: XSAnyURI                 => None
    case item: XSBase64Binary           => Some(Data.Binary(ImmutableArray.fromArray(item.asBinaryData)))
    case item: XSBoolean                => Some(Data.Bool(item.asPrimitiveBoolean))
    case item: XSDate                   => None
    case item: XSDateTime               => None
    case item: XSDecimal                => Some(Data.Dec(item.asBigDecimal))
    case item: XSDouble                 => Some(Data.Dec(item.asBigDecimal))
    case item: XSDuration               => None
    case item: XSFloat                  => Some(Data.Dec(item.asBigDecimal))
    case item: XSGDay                   => None
    case item: XSGMonth                 => None
    case item: XSGMonthDay              => None
    case item: XSGYear                  => None
    case item: XSGYearMonth             => None
    case item: XSHexBinary              => Some(Data.Binary(ImmutableArray.fromArray(item.asBinaryData)))
    case item: XSInteger                => Some(Data.Int(item.asBigInteger))
    case item: XSQName                  => None
    case item: XSString                 => Some(Data.Str(item.asString))
    case item: XSTime                   => None
    case item: XSUntypedAtomic          => None
    case _                              => None
  }
}
