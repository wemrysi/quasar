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

import quasar.api.destination.param.Param
import quasar.api.table.ColumnType
import quasar.fp.Dependent

import scala.{List, Nothing, Option, Product, Some, Serializable, StringContext}
import scala.util.{Either, Left, Right}

import skolems.∃

sealed trait TypeCoercion[+C[_], +T] extends Product with Serializable

object TypeCoercion {
  import Label.Syntax._

  implicit def equal[C[_], T: Eq](implicit C: Eq[∃[C]], P: Dependent[C, Eq]): Eq[TypeCoercion[C, T]] =
    Eq instance {
      case (Unsatisfied(a1, t1), Unsatisfied(a2, t2)) =>
        a1 === a2 && t1 === t2

      case (s1: Satisfied[C, T], s2: Satisfied[C, T]) =>
        s1.priority === s2.priority

      case _ =>
        false
    }

  implicit def show[C[_], T: Show](implicit C: Show[∃[C]], P: Dependent[C, Show]): Show[TypeCoercion[C, T]] =
    Show show {
      case Unsatisfied(alt, top) =>
        s"Unsatisfied(${alt.show}, ${top.show})"

      case s: Satisfied[C, T] =>
        s"Satisfied(${s.priority.show})"
    }

  // this lives here because it must be uniform with preappliedEncodeJson
  implicit def appliedDecodeJson[C[_], T: DecodeJson](
      implicit C: DecodeJson[∃[C]], P: Dependent[C, DecodeJson])
      : DecodeJson[Either[∃[λ[α => (C[α], α)]], T]] =
    DecodeJson { c =>
      for {
        tpeOrC <- (c --\ "type").as[Either[∃[C], T]]

        back <- tpeOrC match {
          case Left(const) =>
            (c --\ "params" =\ 0).as(P(const.value)) map { p =>
              Left(∃[λ[α => (C[α], α)]]((const.value, p)))
            }

          case Right(t) =>
            DecodeResult.ok(Right(t))
        }
      } yield back
    }

  implicit def preappliedEncodeJson[C[_], T: EncodeJson: Label](
      implicit CE: EncodeJson[∃[C]],
      PE: Dependent[C, EncodeJson],
      CS: Label[∃[C]])
      : EncodeJson[Either[Labeled[Unapplied[C]], T]] = {
    EncodeJson {
      // we encode constructors as types with params
      case Left(Labeled(plabel, un)) =>
        implicit val ev = PE(un.const)
        val const = ∃(un.const)

        Json(
          "label" := const.label,
          "type" := const,
          "params" := Json.array(Json(plabel := un.param)))

      case Right(t) =>
        Json(
          "label" := t.label,
          "type" := t)
    }
  }

  implicit def encodeJson[C[_], T: EncodeJson: Label](
      implicit ct: EncodeJson[ColumnType],
      CE: EncodeJson[∃[C]],
      PE: Dependent[C, EncodeJson],
      CS: Label[∃[C]])
      : EncodeJson[TypeCoercion[C, T]] =
    EncodeJson {
      case Unsatisfied(alternatives, top) =>
        top.map(t => "top" := t) ->?: Json("alternatives" := alternatives)

      case s: Satisfied[C, T] =>
        jArray(s.priority.toList.map(_.asJson))
    }

  trait Unapplied[C[_]] {
    type P
    val const: C[P]
    val param: Param[P]
  }

  object Unapplied {
    type Aux[C[_], P0] = Unapplied[C] { type P = P0 }

    // lowers the type since it's irrelevant
    def apply[C[_], P0](const0: C[P0], param0: Param[P0]): Unapplied[C] =
      new Unapplied[C] { type P = P0; val const = const0; val param = param0 }

    def unapply[C[_]](u: Unapplied[C]): Some[(C[u.P], Param[u.P])] =
      Some((u.const, u.param))

    implicit def equal[C[_]](implicit C: Eq[∃[C]], P: Dependent[C, Eq]): Eq[Unapplied[C]] =
      Eq instance { (u1, u2) =>
        val c1 = ∃(u1.const)
        val c2 = ∃(u2.const)

        if (c1 === c2) {
          implicit val eqP = P(u1.const)
          u1.param === u2.param.asInstanceOf[Param[u1.P]]
        } else {
          false
        }
      }

    implicit def show[C[_]](implicit C: Show[∃[C]], P: Dependent[C, Show]): Show[Unapplied[C]] =
      Show show {
        case Unapplied(c, p) =>
          implicit val showP = P(c)
          s"Unapplied(${∃(c).show}, ${p.show})"
      }
  }

  final case class Unsatisfied[T](
      alternatives: List[ColumnType],
      top: Option[T])
      extends TypeCoercion[Nothing, T]

  final case class Satisfied[C[_], T](
      priority: NonEmptyList[Either[Labeled[Unapplied[C]], T]])
      extends TypeCoercion[C, T]
}
