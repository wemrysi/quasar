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

package quasar.api.destination

import argonaut._, Argonaut._

import cats.{Eq, Show}
import cats.data.NonEmptyList
import cats.implicits._

import quasar.fp.{τ, Dependent}
import quasar.api.destination.param.Param
import quasar.api.table.ColumnType

import scala.{List, Nil, Nothing, Option, Predef, Product, Serializable, StringContext}, Predef._
import scala.util.{Either, Left, Right}

sealed trait TypeCoercion[+C[_], +T] extends Product with Serializable

object TypeCoercion {
  import Label.Syntax._

  implicit def equal[C[_], T: Eq](implicit C: Eq[C[τ]], P: Dependent[C, Eq]): Eq[TypeCoercion[C, T]] =
    Eq instance {
      case (Unsatisfied(a1, t1), Unsatisfied(a2, t2)) =>
        a1 === a2 && t1 === t2

      case (s1: Satisfied[C, T], s2: Satisfied[C, T]) =>
        s1.priority === s2.priority

      case _ =>
        false
    }

  implicit def show[C[_], T: Show](implicit C: Show[C[τ]], P: Dependent[C, Show]): Show[TypeCoercion[C, T]] =
    Show show {
      case Unsatisfied(alt, top) =>
        s"Unsatisfied(${alt.show}, ${top.show})"

      case s: Satisfied[C, T] =>
        s"Satisfied(${s.priority.show})"
    }

  // this lives here because it must be uniform with preappliedEncodeJson
  implicit def appliedDecodeJson[C[_], T: DecodeJson](
      implicit C: DecodeJson[C[τ]],
      P: Dependent[C, DecodeJson])
      : DecodeJson[Either[(C[P], P) forSome { type P }, T]] =
    DecodeJson { c =>
      for {
        tpeOrC <- (c --\ "type").as[Either[C[τ], T]]

        back <- tpeOrC match {
          case Left(const) =>
            (c --\ "params" =\ 0).as(P(const)).map((const, _)).map(Left(_))

          case Right(t) =>
            DecodeResult.ok(Right(t))
        }
      } yield back
    }

  implicit def preappliedEncodeJson[C[_], T: EncodeJson: Label](
      implicit CE: EncodeJson[C[τ]],
      PE: Dependent[C, EncodeJson],
      CS: Label[C[τ]],
      PS: Dependent[C, Label])
      : EncodeJson[Either[Labeled[Unapplied[C, _]], T]] = {
    EncodeJson {
      // we encode constructors as types with params
      case Left(Labeled(plabel, un)) =>
        // normalize the skolems to hold scalac's hand
        val const = un.const.asInstanceOf[C[τ]]
        val param = un.param.asInstanceOf[Param[τ]]

        implicit val pe = PE(const)

        Json(
          "label" -> const.label.asJson,
          "type" -> const.asJson,
          "params" -> jArray(Json(plabel -> param.asJson) :: Nil))

      case Right(t) =>
        Json(
          "label" -> t.label.asJson,
          "type" -> t.asJson)
    }
  }

  implicit def encodeJson[C[_], T: EncodeJson: Label](
      implicit ct: EncodeJson[ColumnType],
      CE: EncodeJson[C[τ]],
      PE: Dependent[C, EncodeJson],
      CS: Label[C[τ]],
      PS: Dependent[C, Label])
      : EncodeJson[TypeCoercion[C, T]] =
    EncodeJson {
      case Unsatisfied(alternatives, top) =>
        Json(List("alternatives" -> alternatives.asJson) ++ top.map(t => "top" -> t.asJson).toList: _*)

      case s: Satisfied[C, T] =>
        jArray(s.priority.toList.map(_.asJson))
    }

  final case class Unapplied[C[_], P](const: C[P], param: Param[P])

  object Unapplied {
    import τ.materialize

    implicit def equal[C[_]](implicit C: Eq[C[τ]], P: Dependent[C, Eq]): Eq[Unapplied[C, _]] =
      Eq instance { (_u1, _u2) =>
        // we forcibly normalize the skolems here because we'll be doing a runtime check below to make *sure*
        val u1 = _u1.asInstanceOf[Unapplied[C, τ]]
        val u2 = _u2.asInstanceOf[Unapplied[C, τ]]

        implicit val eqP = P(u1.const)
        u1.const === u2.const && u1.param === u2.param
      }

    implicit def show[C[_]](implicit C: Show[C[τ]], P: Dependent[C, Show]): Show[Unapplied[C, _]] =
      Show show {
        case Unapplied(c, p) =>
          implicit val showP = P(c)
          s"Unapplied(${c.show}, ${p.show})"
      }
  }

  final case class Unsatisfied[T](
      alternatives: List[ColumnType],
      top: Option[T])
      extends TypeCoercion[Nothing, T]

  final case class Satisfied[C[_], T](
      priority: NonEmptyList[Either[Labeled[Unapplied[C, _]], T]])
      extends TypeCoercion[C, T]
}
