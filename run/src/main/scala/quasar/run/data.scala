/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.run

import slamdata.Predef.{Array, SuppressWarnings}
import quasar.precog.common._

import argonaut._, Argonaut._

object data {
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def jsonToRValue(js: Json): RValue =
    js.fold(
      jsonNull = RValue.rNull(),
      jsonBool = RValue.rBoolean(_),
      jsonNumber = n => RValue.rNum(n.toBigDecimal),
      jsonString = RValue.rString(_),
      jsonArray = elems => RValue.rArray(elems.map(jsonToRValue)),
      jsonObject = fields => RValue.rObject(fields.toMap.mapValues(jsonToRValue)))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def rValueToJson(rv: RValue): Json =
    rv match {
      case RObject(fields) => jObjectAssocList(fields.mapValues(rValueToJson).toList)
      case CEmptyObject => jEmptyObject
      case RArray(elems) => jArray(elems.map(rValueToJson))
      case CEmptyArray => jEmptyArray
      case CArray(_, _) => jEmptyArray
      case CUndefined => jNull
      case CNull => jNull
      case CBoolean(b) => jBool(b)
      case CString(s) => jString(s)
      case CLong(l) => jNumber(l)
      case CDouble(d) => jNumber(d)
      case CNum(n) => jNumber(JsonBigDecimal(n))
      case COffsetDateTime(t) => jString(t.toString)
      case COffsetDate(t) => jString(t.toString)
      case COffsetTime(t) => jString(t.toString)
      case CLocalDateTime(t) => jString(t.toString)
      case CLocalDate(t) => jString(t.toString)
      case CLocalTime(t) => jString(t.toString)
      case CInterval(t) => jString(t.toString)
    }
}
