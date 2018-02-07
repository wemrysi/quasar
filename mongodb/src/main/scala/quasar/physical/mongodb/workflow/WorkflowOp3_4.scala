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

package quasar.physical.mongodb.workflow

import slamdata.Predef._
import quasar.{RenderTree, NonTerminal}
import quasar.physical.mongodb.Reshape
import quasar.physical.mongodb.expression._

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

/** Ops that are provided by MongoDB since 3.4. */
sealed abstract class WorkflowOp3_4F[+A] extends Product with Serializable

final case class $AddFieldsF[A](src: A, shape: Reshape[ExprOp])
    extends WorkflowOp3_4F[A] { self =>
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def pipeline: PipelineF[WorkflowOp3_4F, A] =
    new PipelineF[WorkflowOp3_4F, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

      def op = "$addFields"
      def rhs = shape.bson
    }


}

object $addFields {
  def apply[F[_]: Coalesce](shape: Reshape[ExprOp])
    (implicit I: WorkflowOp3_4F :<: F)
      : FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($AddFieldsF(src, shape))))
}

object WorkflowOp3_4F {
  implicit val traverse: Traverse[WorkflowOp3_4F] =
    new Traverse[WorkflowOp3_4F] {
      def traverseImpl[G[_], A, B](fa: WorkflowOp3_4F[A])(f: A => G[B])
        (implicit G: Applicative[G]):
          G[WorkflowOp3_4F[B]] = fa match {
        case $AddFieldsF(src, shape) => G.apply(f(src))($AddFieldsF(_, shape))
      }
    }

  implicit val refs: Refs[WorkflowOp3_4F] = Refs.fromRewrite[WorkflowOp3_4F](rewriteRefs3_4)

  implicit val crush: Crush[WorkflowOp3_4F] = Crush.injected[WorkflowOp3_4F, WorkflowF]

  implicit val coalesce: Coalesce[Workflow3_4F] = coalesceAll[Workflow3_4F]

  implicit val classify: Classify[WorkflowOp3_4F] =
    new Classify[WorkflowOp3_4F] {
      override def source[A](op: WorkflowOp3_4F[A]) = None

      override def singleSource[A](wf: WorkflowOp3_4F[A]) = wf match {
        case op @ $AddFieldsF(_, _)    => op.pipeline.widen[A].some
      }

      override def pipeline[A](wf: WorkflowOp3_4F[A]) = wf match {
        case op @ $AddFieldsF(_, _)    => op.pipeline.widen[A].some
      }

      override def shapePreserving[A](op: WorkflowOp3_4F[A]) = None
    }

  implicit val renderTree: RenderTree[WorkflowOp3_4F[Unit]] =
    new RenderTree[WorkflowOp3_4F[Unit]] {
      val wfType = "Workflow" :: Nil

      def render(v: WorkflowOp3_4F[Unit]) = v match {
        case $AddFieldsF(_, shape) =>
          NonTerminal("$AddFieldsF" :: wfType, None,
            Reshape.renderReshape(shape))
      }
    }
}
