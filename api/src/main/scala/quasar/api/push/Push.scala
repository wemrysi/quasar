/*
 * Copyright 2020 Precog Data
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

package quasar.api.push

import slamdata.Predef.{Eq => _, _}

import quasar.api.{Column, ColumnType}
import quasar.api.resource.ResourcePath

import cats.{Apply, Eq, Eval, NonEmptyTraverse, Show}
import cats.data.NonEmptyList
import cats.implicits._

import monocle.{PLens, Prism}

import shims.{equalToCats, functorToScalaz, showToCats}

sealed trait Push[O, +Q] extends Product with Serializable {
  def path: ResourcePath
  def query: Q
  def columns: Push.Columns
}

object Push {
  type OutputColumn = Column[(ColumnType.Scalar, SelectedType)]
  type Columns = NonEmptyList[OutputColumn]

  final case class Full[O, Q](
      path: ResourcePath,
      query: Q,
      columns: Columns)
      extends Push[O, Q]

  final case class Incremental[O, Q](
      path: ResourcePath,
      query: Q,
      outputColumns: List[OutputColumn],
      resumeConfig: ResumeConfig[O],
      initialOffset: Option[OffsetKey.Actual[O]])
      extends Push[O, Q] {

    def columns: Columns =
      NonEmptyList(
        resumeConfig.resultIdColumn.map(_.leftMap(IdType.scalarP(_))),
        outputColumns)
  }

  def full[O, Q]: Prism[Push[O, Q], (ResourcePath, Q, Columns)] =
    Prism.partial[Push[O, Q], (ResourcePath, Q, Columns)] {
      case Full(p, q, c) => (p, q, c)
    } ((Full[O, Q] _).tupled)

  def incremental[O, Q]: Prism[Push[O, Q], (ResourcePath, Q, List[OutputColumn], ResumeConfig[O], Option[OffsetKey.Actual[O]])] =
    Prism.partial[Push[O, Q], (ResourcePath, Q, List[OutputColumn], ResumeConfig[O], Option[OffsetKey.Actual[O]])] {
      case Incremental(p, q, c, r, o) => (p, q, c, r, o)
    } ((Incremental[O, Q] _).tupled)

  def query[O, Q1, Q2]: PLens[Push[O, Q1], Push[O, Q2], Q1, Q2] =
    PLens[Push[O, Q1], Push[O, Q2], Q1, Q2](_.query)(q2 => {
      case f @ Full(_, _, _) => f.copy(query = q2)
      case i @ Incremental(_, _, _, _, _) => i.copy(query = q2)
    })

  implicit def pushEq[O, Q: Eq]: Eq[Push[O, Q]] =
    Eq.by(p => (full[O, Q].getOption(p), incremental[O, Q].getOption(p)))

  implicit def pushShow[O, Q: Show]: Show[Push[O, Q]] =
    Show show {
      case Full(p, q, c) =>
        s"Full(${p.show}, ${q.show}, ${c.map(_.name).show})"

      case i @ Incremental(p, q, c, r, o) =>
        s"Incremental(${p.show}, ${q.show}, ${c.map(_.name).show}, ${r.show}, ${o.show})"
    }

  implicit def pushReducible[O]: NonEmptyTraverse[Push[O, ?]] =
    new NonEmptyTraverse[Push[O, ?]] {
      def nonEmptyTraverse[F[_]: Apply, A, B](fa: Push[O, A])(f: A => F[B]) =
        query[O, A, B].modifyF(f)(fa)

      def foldLeft[A, B](fa: Push[O, A], b: B)(f: (B, A) => B): B =
        f(b, query[O, A, A].get(fa))

      def foldRight[A, B](fa: Push[O, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        f(query[O, A, A].get(fa), lb)

      def reduceLeftTo[A, B](fa: Push[O, A])(f: A => B)(g: (B, A) => B): B =
        f(query[O, A, A].get(fa))

      def reduceRightTo[A, B](fa: Push[O, A])(f: A => B)(g: (A, Eval[B]) => Eval[B]): Eval[B] =
        Eval.now(f(query[O, A, A].get(fa)))
    }
}
