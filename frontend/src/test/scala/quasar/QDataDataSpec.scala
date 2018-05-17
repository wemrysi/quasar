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

package quasar

import slamdata.Predef._

import quasar.qdata._

import scalaz.\/

object QDataDataSpec extends Qspec with DataGenerators {
  import QDataData._

  def roundtripUnsafe(data: Data): Data =
    tpe(data) match {
      case QNull => makeNull
      case QString => makeString(getString(data))
      case QBoolean => makeBoolean(getBoolean(data))
      case QReal => makeReal(getReal(data))
      case QDouble => makeDouble(getDouble(data))
      case QLong => makeLong(getLong(data))
      case QOffsetDateTime => makeOffsetDateTime(getOffsetDateTime(data))
      case QOffsetDate => makeOffsetDate(getOffsetDate(data))
      case QOffsetTime => makeOffsetTime(getOffsetTime(data))
      case QLocalDateTime => makeLocalDateTime(getLocalDateTime(data))
      case QLocalDate => makeLocalDate(getLocalDate(data))
      case QLocalTime => makeLocalTime(getLocalTime(data))
      case QInterval => makeInterval(getInterval(data))
      case QBytes => makeBytes(getBytes(data))
      case QArray => makeArray(getArray(data))
      case QObject => makeObject(getObject(data))
    }

  def roundtrip(data: Data): Option[Data] =
    \/.fromTryCatchNonFatal(roundtripUnsafe(data)).toOption

  // Data.Int and Data.Dec type as QLong and QDouble resp. when possible, else default to QReal
  // we don't need to adjust for this case because Equal[Data] compares them as equal
  def adjustExpected(data: Data): Option[Data] = data match {
    case Data.NA => None
    case Data.Id(str) => Some(Data.Str(str))
    case d => Some(d)
  }

  "roundtrip arbitrary Data" >> prop { data: Data =>
    roundtrip(data) must_=== adjustExpected(data)
  }.set(minTestsOk = 1000)
}
