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

import slamdata.Predef._
import quasar.api.datasource._
import quasar.fp.numeric.Positive
import quasar.precog.common._

import java.util.UUID

import argonaut._, Argonaut._
import eu.timepit.refined.refineV
import monocle.Prism
import scalaz.\/
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.traverse._

object optics {

  val RefTypeField = "type"
  val RefTypeVersionField = "typeVersion"
  val RefNameField = "name"
  val RefConfigField = "config"

  def rValueDatasourceRefP[A](cfgP: Prism[RValue, A]): Prism[RValue, DatasourceRef[A]] = {
    def fromRValue(rv: RValue): Option[DatasourceRef[A]] =
      for {
        kind <-
          RValue.rField1(RefTypeField)
            .composePrism(RValue.rString)
            .getOption(rv)

        ver <-
          RValue.rField1(RefTypeVersionField)
            .composePrism(RValue.rNum)
            .getOption(rv)

        name <-
          RValue.rField1(RefNameField)
            .composePrism(RValue.rString)
            .getOption(rv)

        cfg <-
          RValue.rField1(RefConfigField)
            .composePrism(cfgP)
            .getOption(rv)

        tname <- refineV[DatasourceType.NameP](name).toOption

        tver <- Positive(ver.longValue)

        dsType = DatasourceType(tname, tver)
        dsName = DatasourceName(name)
      } yield DatasourceRef(dsType, dsName, cfg)

    val toRValue: DatasourceRef[A] => RValue = {
      case DatasourceRef(DatasourceType(t, v), DatasourceName(n), c) =>
        RValue.rObject(Map(
          RefTypeField -> CString(t.value),
          RefTypeVersionField -> CNum(v.value),
          RefNameField -> CString(n),
          RefConfigField -> cfgP(c)))
    }

    Prism(fromRValue)(toRValue)
  }

  val rValueJsonP: Prism[RValue, Json] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def toRValue(js: Json): RValue =
      js.fold(
        jsonNull = RValue.rNull(),
        jsonBool = RValue.rBoolean(_),
        jsonNumber = n => RValue.rNum(n.toBigDecimal),
        jsonString = RValue.rString(_),
        jsonArray = elems => RValue.rArray(elems.map(toRValue)),
        jsonObject = fields => RValue.rObject(fields.toMap.mapValues(toRValue)))

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def fromRValue(rv: RValue): Option[Json] =
      rv match {
        case CEmptyObject => some(jEmptyObject)
        case RObject(fields) =>
          fields.toList.traverse({
            case (k, v) => fromRValue(v) strengthL k
          }).map(jObjectAssocList)

        case CEmptyArray => some(jEmptyArray)
        case RArray(elems) => elems.traverse(fromRValue).map(jArray)

        case CNull => some(jNull)
        case CBoolean(b) => some(jBool(b))
        case CString(s) => some(jString(s))
        case CLong(l) => some(jNumber(l))
        case CDouble(d) => some(jNumber(d))
        case CNum(n) => some(jNumber(JsonBigDecimal(n)))

        case _ => none
      }

    Prism(fromRValue)(toRValue)
  }

  val stringUUIDP: Prism[String, UUID] =
    Prism[String, UUID](
      s => \/.fromTryCatchNonFatal(UUID.fromString(s)).toOption)(
      u => u.toString)
}
