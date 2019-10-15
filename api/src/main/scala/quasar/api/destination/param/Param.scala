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
import cats.implicits._

import scala.{Boolean, Int, List, Option, None, Predef, Product, Serializable, Some, StringContext}, Predef._
import scala.util.{Either, Left, Right}

sealed trait Param[A]
    extends Product
    with Serializable
    with (Json => Either[ParamError, A])

object Param {

  implicit def equal[A: Eq]: Eq[Param[A]] =
    Eq instance {
      case (Boolean, Boolean) =>
        true

      case (Integer(min1, max1, step1), Integer(min2, max2, step2)) =>
        min1 === min2 && max1 === max2 && step1 === step2

      case (Enum(pos1), Enum(pos2)) =>
        pos1 === pos2

      case _ =>
        false
    }

  implicit def show[A](implicit A: Show[A]): Show[Param[A]] =
    Show show {
      case Boolean => "Boolean"

      case Integer(min, max, step) =>
        val A = null    // shadow the inexplicably gadt-unified instance to avoid ambituity
        s"Integer(${min.show}, ${max.show}, ${step.show})"

      case Enum(possibilities) => s"Enum(${possibilities.show})"
    }

  implicit def encodeJson[A](implicit A: EncodeJson[A]): EncodeJson[Param[A]] =
    EncodeJson {
      case Boolean =>
        Json("type" -> "boolean".asJson)

      case Integer(min, max, step) =>
        val A = null    // shadow the inexplicably gadt-unified instance to avoid ambituity

        Json(
          List(
            List("type" -> "integer".asJson),
            min.toList.map("min" -> _.asJson),
            max.toList.map("max" -> _.asJson),
            step.toList.map("step" -> _.asJson)).flatten: _*)

      case Enum(possibilities) =>
        Json(
          "type" -> "enum".asJson,
          "possibilities" -> possibilities.asJson)
    }

  case object Boolean extends Param[Boolean] {
    def apply(json: Json): Either[ParamError, scala.Boolean] =
      json.bool.map(Right(_)).getOrElse(Left(ParamError.InvalidBoolean(json)))
  }

  final case class Integer(
      min: Option[Int],
      max: Option[Int],
      step: Option[IntegerStep])
      extends Param[Int] {

    def apply(json: Json): Either[ParamError, Int] =
      for {
        i <- json.as[Int].toEither.leftMap(_ => ParamError.InvalidInt(json))

        _ <- if (min.map(i >= _).getOrElse(true) && max.map(i < _).getOrElse(true))
          Right(())
        else
          Left(ParamError.IntOutOfRange(i, min, max))

        _ <- step match {
          case Some(s) =>
            if (s(i))
              Right(())
            else
              Left(ParamError.IntOutOfStep(i, s))

          case None =>
            Right(())
        }
      } yield i
  }

  final case class Enum[A: DecodeJson](possibilities: List[A]) extends Param[A] {
    def apply(json: Json): Either[ParamError, A] =
      for {
        a <- json.as[A].toEither.leftMap(_ => ParamError.InvalidEnum(json))

        _ <- if (possibilities.contains(a))
          Right(())
        else
          Left(ParamError.ValueNotInEnum(a, possibilities))
      } yield a
  }
}
