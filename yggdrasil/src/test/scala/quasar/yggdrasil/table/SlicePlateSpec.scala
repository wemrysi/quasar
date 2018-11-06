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

package quasar.yggdrasil
package table

import quasar.blueeyes.json.JValue
import quasar.common.data.{CBoolean, CLong, CNum, CString, Data, DataGenerators, RObject, RValue}
import quasar.frontend.data.DataCodec
import quasar.precog.JsonTestSupport

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import tectonic.json.Parser

import scala.collection.immutable.ListMap
import scala.math.BigDecimal

object SlicePlateSpec extends Specification with ScalaCheck with DataGenerators {
  import JsonTestSupport._

  "slice plate parsing through tectonic" should {
    "round trip readable" in prop { values: List[JValue] =>
      val input = values.mkString("\n")
      val plate = new SlicePlate(false)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toJsonElements)
          results mustEqual values
      }
    }

    "round trip precise" in prop { values0: List[Data] =>
      val values = values0.map(sortFields(_)).flatMap(stripNA(_))

      val input = values.flatMap(DataCodec.Precise.encode(_)).map(_.nospaces).mkString("\n")
      val plate = new SlicePlate(true)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toRValues).map(RValue.toData(_)).map(sortFields(_))
          results mustEqual values
      }
    }

    "parse Long.MaxValue + 1" in {
      val input = "9223372036854775808"
      val plate = new SlicePlate(true)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toRValues)
          results mustEqual List(CNum(BigDecimal("9223372036854775808")))
      }
    }

    "parse Long.MinValue" in {
      val input = Long.MinValue.toString
      val plate = new SlicePlate(true)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toRValues)
          results mustEqual List(CLong(Long.MinValue))
      }
    }

    "produce slices that are exactly max rows, ideally" in {
      val input = """
        42
        12
        10
        """

      val plate = new SlicePlate(true, defaultMinRows = 2, maxSliceRows = 2)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val combined = slices1 ++ slices2
          combined must haveSize(2)
          combined(0).size mustEqual 2
          combined(1).size mustEqual 1

          val results = combined.flatMap(_.toRValues)
          results mustEqual List(CLong(42), CLong(12), CLong(10))
      }
    }

    "produce slices that are just over max columns, ideally" in {
      val input = """
        { "a": 42, "b": true }
        { "a": 84, "b": true }
        { "a": 12, "c": "baz" }
        { "a": 10, "b": false, "c": "qux" }
        """

      val plate = new SlicePlate(true, maxSliceColumns = 2)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val combined = slices1 ++ slices2
          combined must haveSize(2)
          combined(0).size mustEqual 3
          combined(1).size mustEqual 1

          val results = combined.flatMap(_.toRValues)
          results mustEqual List(
            RObject("a" -> CLong(42), "b" -> CBoolean(true)),
            RObject("a" -> CLong(84), "b" -> CBoolean(true)),
            RObject("a" -> CLong(12), "c" -> CString("baz")),
            RObject("a" -> CLong(10), "b" -> CBoolean(false), "c" -> CString("qux")))
      }
    }
  }

  private[this] def sortFields(data: Data): Data = data match {
    case Data.Obj(fields) =>
      val fields2 = ListMap(fields.mapValues(sortFields(_)).toList.sortBy(_._1): _*)
      Data.Obj(fields2)

    case Data.Arr(values) =>
      Data.Arr(values.map(sortFields(_)))

    case other => other
  }

  private[this] def stripNA(data: Data): Option[Data] = data match {
    case Data.Obj(fields) =>
      val fields2 = fields flatMap {
        case (key, value) => stripNA(value).map(key -> _)
      }

      Some(Data.Obj(fields2))

    case Data.Arr(values) =>
      Some(Data.Arr(values.flatMap(stripNA(_))))

    case Data.NA => None

    case other => Some(other)
  }
}
