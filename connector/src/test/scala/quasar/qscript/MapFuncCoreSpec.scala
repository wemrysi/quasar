/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.qscript

import slamdata.Predef._
import quasar.{Qspec, TreeMatchers}
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp.{coproductEqual, coproductShow}

import matryoshka.{delayEqual, delayShow}
import matryoshka.data.Fix
import matryoshka.data.free._
import matryoshka.implicits._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._

final class MapFuncCoreSpec extends Qspec with TTypes[Fix] with TreeMatchers {
  import MapFuncCore.{ConcatMapsN, StaticMap, StaticMapSuffix}

  val ejs = Fixed[Fix[EJson]]
  val func = construction.Func[Fix]

  val staticAssocs =
    List(
      ejs.str("foo") -> func.Add(func.ProjectKeyS(func.Hole, "bar"), func.Constant(ejs.int(3)))
    , ejs.int(1)     -> func.ProjectKeyS(func.Hole, "baz")
    , ejs.char('n')  -> func.Now[Hole])

  val staticMapExpr =
      func.ConcatMaps(
        func.ConcatMaps(
          func.MakeMapS(
            "foo",
            func.Add(func.ProjectKeyS(func.Hole, "bar"), func.Constant(ejs.int(3)))),
          func.MakeMap(
            func.Constant(ejs.int(1)),
            func.ProjectKeyS(func.Hole, "baz"))),
        func.MakeMap(
          func.Constant(ejs.char('n')),
          func.Now))

  "StaticMap" >> {
    "constructor should construct a map with static keys" >> {
      StaticMap(staticAssocs) must beTreeEqual(staticMapExpr)
    }

    "extractor should returns static key/value pairs" >> {
      StaticMap.unapply(staticMapExpr.project) must beSome(equal(staticAssocs))
    }

    "extractor should fail when not completely static" >> {
      val dynamic =
        func.ConcatMaps(
          staticMapExpr,
          func.ProjectKeyS(func.Hole, "dyn"))

      StaticMap.unapply(dynamic.project) must beNone
    }
  }

  "ConcatMapsN" >> {
    val constMap =
      ejs.map(List(
        ejs.int(1)    -> ejs.str("foo")
      , ejs.char('b') -> ejs.dec(1.2)))

    val mapExprs =
      List(
        func.MakeMapS("baz", func.Now[Hole])
      , func.ProjectKeyS(func.Hole, "map1")
      , func.Constant[Hole](constMap))

    val concatExpr =
      func.ConcatMaps(
        func.ConcatMaps(
          func.MakeMapS("baz", func.Now)
        , func.ProjectKeyS(func.Hole, "map1"))
      , func.Constant(constMap))

    "constructor concatenates arguments" >> {
      ConcatMapsN(mapExprs).embed must beTreeEqual(concatExpr)
    }

    "extractor returns individual maps" >> {
      ConcatMapsN.unapply(concatExpr.project) must beSome(equal(mapExprs))
    }

    "extractor matches a single makeMap" >> {
      val x = func.MakeMapS("foo", func.Hole)
      ConcatMapsN.unapply(x.project) must beSome(equal(List(x)))
    }

    "extractor matches a constant map" >> {
      val x = func.Constant[Hole](constMap)
      ConcatMapsN.unapply(x.project) must beSome(equal(List(x)))
    }
  }

  "StaticMapSuffix" >> {
    "equals StaticMap when expression is a static map" >> {
      val x = staticMapExpr.project
      StaticMapSuffix.unapply(x) must_= StaticMap.unapply(x).map((None, _))
    }

    "emits no static part when map is completely dynamic" >> {
      val x = func.ConcatMaps(func.Hole, func.ProjectKeyS(func.Hole, "baz"))
      StaticMapSuffix.unapply(x.project) must_= Some((Some(x), List()))
    }

    "emit static suffix when present" >> {
      val dynMap =
        func.ProjectKeyS(func.Hole, "dyn")

      val mixed =
        func.ConcatMaps(dynMap, staticMapExpr)

      StaticMapSuffix.unapply(mixed.project) must_= Some((Some(dynMap), staticAssocs))
    }
  }
}
