/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.api.destination.param

import argonaut._, Argonaut._

import cats.{Eq, Show}

import scala.{math, Boolean, Int, Predef, Product, Serializable, StringContext}, Predef._

sealed trait IntegerStep extends Product with Serializable with (Int => Boolean)

object IntegerStep {

  implicit val equal: Eq[IntegerStep] =
    Eq.fromUniversalEquals[IntegerStep]

  implicit val show: Show[IntegerStep] =
    Show.fromToString[IntegerStep]

  implicit val codecJson: CodecJson[IntegerStep] =
    CodecJson(
      {
        case Factor(init, n) =>
          Json(
            "type" -> "factor".asJson,
            "init" -> init.asJson,
            "n" -> n.asJson)

        case Power(init, n) =>
          Json(
            "type" -> "power".asJson,
            "init" -> init.asJson,
            "n" -> n.asJson)
      },
      { cursor =>
        (cursor --\ "type").as[String] flatMap {
          case "factor" =>
            for {
              init <- (cursor --\ "init").as[Int]
              n <- (cursor --\ "n").as[Int]
            } yield Factor(init, n)

          case "power" =>
            for {
              init <- (cursor --\ "init").as[Int]
              n <- (cursor --\ "n").as[Int]
            } yield Power(init, n)

          case t =>
            DecodeResult.fail[IntegerStep](s"unrecognized param type '$t'", cursor.history)
        }
      })

  // NB: I have no interest in encoding more complex polynomials

  final case class Factor(init: Int, n: Int) extends IntegerStep {
    def apply(i: Int): Boolean =
      i % n == init
  }

  final case class Power(init: Int, n: Int) extends IntegerStep {
    def apply(i: Int): Boolean =
      math.round(math.log(i.toDouble) / math.log(n.toDouble)) == init
  }
}
