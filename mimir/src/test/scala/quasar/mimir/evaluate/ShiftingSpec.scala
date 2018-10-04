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

  import Shifting.{compositeValueAtPath, shiftRValue, ShiftInfo}

  "rvalue shifting" >> {

    "return entire array when provided path is empty and shift type is array" >> {
      val target =
        RArray(List(CString("target"), CDouble(1.2)))

      compositeValueAtPath(List(), ShiftType.Array, target) must equal(Some(target))
    }

    "return entire object when provided path is empty and shift type is object" >> {
      val target =
        RObject(("target", CDouble(1.2)))

      compositeValueAtPath(List(), ShiftType.Map, target) must equal(Some(target))
    }

    "return no results when the provided path is not present" >> {
      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", RObject(("shiftpath", CDouble(1.2)))))

      val shiftPath = ShiftPath(List("shiftpath"))
      val shiftKey = ShiftKey("shiftkey")

      val shiftInfo = ShiftInfo(shiftPath, IncludeId, ShiftType.Map, shiftKey)

      compositeValueAtPath(shiftPath.path, shiftInfo.shiftType, rvalue) must equal(None)

      shiftRValue(rvalue, shiftInfo) must equal(List())
    }

    "return no results when the provided rvalue is an array" >> {
      val rvalue =
        RArray(
          CLong(4L),
          CBoolean(true),
          RObject(("target", CDouble(1.2))))

      compositeValueAtPath(List("target"), ShiftType.Map, rvalue) must equal(None)
    }

    "return no results when the provided rvalue is a constant" >> {
      val rvalue = CLong(4L)

      compositeValueAtPath(List("target"), ShiftType.Map, rvalue) must equal(None)
    }

    "return correct results when the provided path points to an object at the top level" >> {
      val target =
        RObject(("target", CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", target))

      compositeValueAtPath(List("three"), ShiftType.Map, rvalue) must equal(Some(target))
    }

    "return no results when the provided path points to an object but the shift type is array" >> {
      val target =
        RObject(("target", CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", target))

      compositeValueAtPath(List("three"), ShiftType.Array, rvalue) must equal(None)
    }

    "return correct results when the provided path points to an array at the top level" >> {
      val target =
        RArray(List(CString("target"), CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", target))

      compositeValueAtPath(List("three"), ShiftType.Array, rvalue) must equal(Some(target))
    }

    "return no results when the provided path points to an array but the shift type is an object" >> {
      val target =
        RArray(List(CString("target"), CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", target))

      compositeValueAtPath(List("three"), ShiftType.Map, rvalue) must equal(None)
    }

    "return no results when the provided path is two levels deep and is not an object" >> {
      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", RObject(("target", CDouble(1.2)))))

      compositeValueAtPath(List("three", "target"), ShiftType.Map, rvalue) must equal(None)
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

      val shiftInfoIncludeId = ShiftInfo(shiftPath, IncludeId, ShiftType.Map, shiftKey)
      val shiftInfoExcludeId = ShiftInfo(shiftPath, ExcludeId, ShiftType.Map, shiftKey)
      val shiftInfoIdOnly = ShiftInfo(shiftPath, IdOnly, ShiftType.Map, shiftKey)

      compositeValueAtPath(shiftPath.path, ShiftType.Map, rvalue) must equal(Some(target))

      shiftRValue(rvalue, shiftInfoIncludeId) must equal(
        List(RObject((shiftKey.key, RArray(CString("target"), CDouble(1.2))))))

      shiftRValue(rvalue, shiftInfoExcludeId) must equal(
        List(RObject((shiftKey.key, CDouble(1.2)))))

      shiftRValue(rvalue, shiftInfoIdOnly) must equal(
        List(RObject((shiftKey.key, CString("target")))))
    }

    "return correct results when the provided path is two levels deep and is an array" >> {
      val target =
        RArray(List(CString("target"), CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", RObject(("three-nested", target))))

      val shiftPath = ShiftPath(List("three", "three-nested"))
      val shiftKey = ShiftKey("shiftkey")

      val shiftInfoIncludeId = ShiftInfo(shiftPath, IncludeId, ShiftType.Array, shiftKey)
      val shiftInfoExcludeId = ShiftInfo(shiftPath, ExcludeId, ShiftType.Array, shiftKey)
      val shiftInfoIdOnly = ShiftInfo(shiftPath, IdOnly, ShiftType.Array, shiftKey)

      compositeValueAtPath(shiftPath.path, ShiftType.Array, rvalue) must equal(Some(target))

      shiftRValue(rvalue, shiftInfoIncludeId) must equal(
        List(
          RObject((shiftKey.key, RArray(CLong(0), CString("target")))),
          RObject((shiftKey.key, RArray(CLong(1), CDouble(1.2))))))

      shiftRValue(rvalue, shiftInfoExcludeId) must equal(
        List(
          RObject((shiftKey.key, CString("target"))),
          RObject((shiftKey.key, CDouble(1.2)))))

      shiftRValue(rvalue, shiftInfoIdOnly) must equal(
        List(
          RObject((shiftKey.key, CLong(0))),
          RObject((shiftKey.key, CLong(1)))))
    }

    "return correct results when the provided path is three levels deep and is an object with three fields" >> {
      val target =
        RObject(
          ("target0", CDouble(1.0)),
          ("target1", CDouble(1.1)),
          ("target2", CDouble(1.2)))

      val rvalue =
        RObject(
          ("one", CLong(4L)),
          ("two", CBoolean(true)),
          ("three", RObject(("three-nested", RObject(("three-nested-nested", target))))))

      val shiftPath = ShiftPath(List("three", "three-nested", "three-nested-nested"))
      val shiftKey = ShiftKey("shiftkey")

      val shiftInfoIncludeId = ShiftInfo(shiftPath, IncludeId, ShiftType.Map, shiftKey)
      val shiftInfoExcludeId = ShiftInfo(shiftPath, ExcludeId, ShiftType.Map, shiftKey)
      val shiftInfoIdOnly = ShiftInfo(shiftPath, IdOnly, ShiftType.Map, shiftKey)

      compositeValueAtPath(shiftPath.path, ShiftType.Map, rvalue) must equal(Some(target))

      shiftRValue(rvalue, shiftInfoIncludeId) must equal(
        List(
          RObject((shiftKey.key, RArray(CString("target2"), CDouble(1.2)))),
          RObject((shiftKey.key, RArray(CString("target1"), CDouble(1.1)))),
          RObject((shiftKey.key, RArray(CString("target0"), CDouble(1.0))))))

      shiftRValue(rvalue, shiftInfoExcludeId) must equal(
        List(
          RObject((shiftKey.key, CDouble(1.2))),
          RObject((shiftKey.key, CDouble(1.1))),
          RObject((shiftKey.key, CDouble(1.0)))))

      shiftRValue(rvalue, shiftInfoIdOnly) must equal(
        List(
          RObject((shiftKey.key, CString("target2"))),
          RObject((shiftKey.key, CString("target1"))),
          RObject((shiftKey.key, CString("target0")))))
    }
  }
}
