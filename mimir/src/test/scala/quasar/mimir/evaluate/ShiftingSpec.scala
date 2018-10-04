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

package quasar.mimir.evaluate

import quasar.Qspec
import quasar.precog.common._
import quasar.qscript._

import scalaz.std.list._
import scalaz.std.option._

object ShiftingSpec extends Qspec {

  "rvalue shifting" >> {

    import Shifting.{compositeValueAtPath, shiftRValue, ShiftInfo}

    "return no results when the provided path is not present" >> {
      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", RObject(("shiftpath", CDouble(1.2)))))

      val shiftPath = ShiftPath(List("shiftpath"))
      val shiftKey = ShiftKey("shiftkey")

      val shiftInfo = ShiftInfo(shiftPath, IncludeId, shiftKey)

      compositeValueAtPath(shiftPath.path, rvalue) must equal(None)

      shiftRValue(rvalue, shiftInfo) must equal(List())
    }

    "return no results when the provided rvalue is an array" >> {
      val rvalue =
        RArray(
          CLong(4L),
          CBoolean(true),
          RObject(("target", CDouble(1.2))))

      compositeValueAtPath(List("target"), rvalue) must equal(None)
    }

    "return no results when the provided rvalue is a constant" >> {
      val rvalue = CLong(4L)

      compositeValueAtPath(List("target"), rvalue) must equal(None)
    }

    "return correct results when the provided path is at the top level" >> {
      val target =
        RObject(("target", CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", target))

      compositeValueAtPath(List("three"), rvalue) must equal(Some(target))
    }

    "return no results when the provided path is two levels deep and is not an object" >> {
      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", RObject(("target", CDouble(1.2)))))

      compositeValueAtPath(List("three", "target"), rvalue) must equal(None)
    }

    "return correct results when the provided path is two levels deep and is an object" >> {
      val target =
        RObject(("target", CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", RObject(("three-nested", target))))

      val shiftPath = ShiftPath(List("three", "three-nested"))
      val shiftKey = ShiftKey("shiftkey")

      val shiftInfoIncludeId = ShiftInfo(shiftPath, IncludeId, shiftKey)
      val shiftInfoExcludeId = ShiftInfo(shiftPath, ExcludeId, shiftKey)
      val shiftInfoIdOnly = ShiftInfo(shiftPath, IdOnly, shiftKey)

      compositeValueAtPath(shiftPath.path, rvalue) must equal(Some(target))

      shiftRValue(rvalue, shiftInfoIncludeId) must equal(
        List(RObject((shiftKey.key, RArray(CString("target"), CDouble(1.2))))))

      shiftRValue(rvalue, shiftInfoExcludeId) must equal(
        List(RObject((shiftKey.key, CDouble(1.2)))))

      shiftRValue(rvalue, shiftInfoIdOnly) must equal(
        List(RObject((shiftKey.key, CString("target")))))
    }

    "return correct results when the provided path is three levels deep and is an object" >> {
      val target =
        RObject(("target", CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", RObject(("three-nested", RObject(("three-nested-nested", target))))))

      val shiftPath = ShiftPath(List("three", "three-nested", "three-nested-nested"))
      val shiftKey = ShiftKey("shiftkey")

      val shiftInfoIncludeId = ShiftInfo(shiftPath, IncludeId, shiftKey)
      val shiftInfoExcludeId = ShiftInfo(shiftPath, ExcludeId, shiftKey)
      val shiftInfoIdOnly = ShiftInfo(shiftPath, IdOnly, shiftKey)

      compositeValueAtPath(shiftPath.path, rvalue) must equal(Some(target))

      shiftRValue(rvalue, shiftInfoIncludeId) must equal(
        List(RObject((shiftKey.key, RArray(CString("target"), CDouble(1.2))))))

      shiftRValue(rvalue, shiftInfoExcludeId) must equal(
        List(RObject((shiftKey.key, CDouble(1.2)))))

      shiftRValue(rvalue, shiftInfoIdOnly) must equal(
        List(RObject((shiftKey.key, CString("target")))))
    }
  }
}
