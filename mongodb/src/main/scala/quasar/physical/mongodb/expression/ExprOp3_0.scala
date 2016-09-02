/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.mongodb.expression0 // HACK

import quasar.Predef._
import quasar._, Planner._
import quasar.fp._
import quasar.physical.mongodb.Bson
import quasar.physical.mongodb.expression.DocVar // HACK
import quasar.jscore, jscore.JsFn

import matryoshka._
import scalaz._, Scalaz._

/** "Pipeline" operators added in MongoDB version 3.0. */
trait ExprOp3_0F[A]
object ExprOp3_0F {
  final case class $dateToStringF[A](format: FormatString, date: A) extends ExprOp3_0F[A]

  // TODO: if this is needed, comment explaining why
  def unapply[EX[_], A](ex: EX[A])(implicit I: ExprOp3_0F :<: EX): Option[ExprOp3_0F[A]] =
    I.prj(ex)

  implicit val equal:
      Delay[Equal, ExprOp3_0F] =
    new Delay[Equal, ExprOp3_0F] {
      def apply[A](eq: Equal[A]) = {
        implicit val A = eq
        Equal.equal {
          case ($dateToStringF(fmt1, x1), $dateToStringF(fmt2, x2)) =>
            fmt1 ≟ fmt2 && x1 ≟ x2
        }
      }
    }

  implicit val traverse: Traverse[ExprOp3_0F] = new Traverse[ExprOp3_0F] {
    def traverseImpl[G[_], A, B](fa: ExprOp3_0F[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[ExprOp3_0F[B]] =
      fa match {
        case $dateToStringF(fmt, v) => G.map(f(v))($dateToStringF(fmt, _))
      }
  }

  implicit val ops: ExprOpOps[ExprOp3_0F] = new ExprOpOps[ExprOp3_0F] {

    def simplify[F[_]](implicit inj: Inj[ExprOp3_0F, F]): AlgebraM[Option, ExprOp3_0F, Fix[F]] =
      κ(None)

    def bson: Algebra[ExprOp3_0F, Bson] = {
      case $dateToStringF(format, date) =>
        Bson.Doc("$dateToString" -> Bson.Doc(
          "format" -> Bson.Text(format.components.foldMap(_.fold(_.replace("%", "%%"), _.str))),
          "date" -> date))
    }

    def toJsSimple: AlgebraM[PlannerError \/ ?, ExprOp3_0F, JsFn] =
      // TODO: it's not clear that this will be needed prior to swtiching to the QScript backend
      expr => UnsupportedJS(expr.toString).left

    def rewriteRefs0[F[_]: Functor](applyVar: PartialFunction[DocVar, DocVar])(implicit inj: Inj[ExprOp3_0F, F]) =
      κ(None)
  }

  final case class fixpoint[T[_[_]]: Corecursive, EX[_]: Functor](implicit val inj: Inj[ExprOp3_0F, EX]) {
    def $dateToString(format: FormatString, date: T[EX]): T[EX] =
      inj($dateToStringF(format, date)).embed
  }
}

sealed abstract class FormatSpecifier(val str: String)
object FormatSpecifier {
  case object Year        extends FormatSpecifier("%Y")
  case object Month       extends FormatSpecifier("%m")
  case object DayOfMonth  extends FormatSpecifier("%d")
  case object Hour        extends FormatSpecifier("%H")
  case object Minute      extends FormatSpecifier("%M")
  case object Second      extends FormatSpecifier("%S")
  case object Millisecond extends FormatSpecifier("%L")
  case object DayOfYear   extends FormatSpecifier("%j")
  case object DayOfWeek   extends FormatSpecifier("%w")
  case object WeekOfYear  extends FormatSpecifier("%U")
}

final case class FormatString(components: List[String \/ FormatSpecifier]) {
  def ::(str: String): FormatString = FormatString(str.left :: components)
  def ::(spec: FormatSpecifier): FormatString = FormatString(spec.right :: components)
}
object FormatString {
  val empty: FormatString = FormatString(Nil)

  implicit val equal: Equal[FormatString] = Equal.equalA
}

object $dateToStringF {
  def apply[EX[_], A](format: FormatString, date: A)(implicit inj: Inj[ExprOp3_0F, EX]): EX[A] =
    inj(ExprOp3_0F.$dateToStringF(format, date))
  def unapply[EX[_], A](expr: EX[A])(implicit prj: Prj[ExprOp3_0F, EX]): Option[(FormatString, A)] =
    prj(expr) collect {
      case ExprOp3_0F.$dateToStringF(format, date) => (format, date)
    }
}

object $dateToString {
  def unapply[T[_[_]]: Recursive, EX[_]: Functor](expr: T[EX])(implicit prj: Prj[ExprOp3_0F, EX]): Option[(FormatString, T[EX])] =
    $dateToStringF.unapply(Recursive[T].project(expr))
}
