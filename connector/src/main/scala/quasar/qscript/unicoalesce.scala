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
import quasar.fp._
import quasar.fp.ski._

import matryoshka._
import scalaz._

sealed trait Unicoalesce[T[_[_]], C <: CoM] {
  def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]): C#M[T[C#M]] => Option[C#M[T[C#M]]]
}

object Unicoalesce {

  def apply[T[_[_]], C <: CoM](
    implicit
      C: Coalesce.Aux[T, C#M, C#M],
      F: Functor[C#M],
      QC: UnicoalesceQC[T, C],
      SR: UnicoalesceSR[T, C],
      EJ: UnicoalesceEJ[T, C],
      TJ: UnicoalesceTJ[T, C],
      N: Normalizable[C#M]): C#M[T[C#M]] => C#M[T[C#M]] = {

    val cs = List(
      QC(C),
      SR(C),
      EJ(C),
      TJ(C),
      N.normalizeF(_: C#M[T[C#M]]))

    cs match {
      case hd :: tl =>
        repeatedly(
          applyTransforms[T, C#M, C#M](hd, tl: _*))

      case Nil => (x => x)
    }
  }
}

sealed trait UnicoalesceQC[T[_[_]], C <: CoM] extends Unicoalesce[T, C]

private[qscript] trait UnicoalesceQCLowPriorityImplicits {
  implicit def default[T[_[_]], C <: CoM]: UnicoalesceQC[T, C] = new UnicoalesceQC[T, C] {
    def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]) = κ(None)
  }
}

object UnicoalesceQC extends UnicoalesceQCLowPriorityImplicits {

  implicit def member[T[_[_]], C <: CoM](
    implicit
      QC: QScriptCore[T, ?] :<: C#M): UnicoalesceQC[T, C] = new UnicoalesceQC[T, C] {

    def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]) =
      C.coalesceQC[C#M](idPrism)
  }
}

sealed trait UnicoalesceSR[T[_[_]], C <: CoM] extends Unicoalesce[T, C]

private[qscript] trait UnicoalesceSRLowPriorityImplicits {
  implicit def default[T[_[_]], C <: CoM]: UnicoalesceSR[T, C] = new UnicoalesceSR[T, C] {
    def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]) = κ(None)
  }
}

object UnicoalesceSR extends UnicoalesceSRLowPriorityImplicits {

  // TODO the A might not infer here
  implicit def member[T[_[_]], A, C <: CoM](
    implicit
      QC: QScriptCore[T, ?] :<: C#M,
      SR: Const[ShiftedRead[A], ?] :<: C#M): UnicoalesceSR[T, C] = new UnicoalesceSR[T, C] {

    def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]) =
      C.coalesceSR[C#M, A](idPrism)
  }
}

sealed trait UnicoalesceEJ[T[_[_]], C <: CoM] extends Unicoalesce[T, C]

private[qscript] trait UnicoalesceEJLowPriorityImplicits {
  implicit def default[T[_[_]], C <: CoM]: UnicoalesceEJ[T, C] = new UnicoalesceEJ[T, C] {
    def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]) = κ(None)
  }
}

object UnicoalesceEJ extends UnicoalesceEJLowPriorityImplicits {

  implicit def member[T[_[_]], C <: CoM](
    implicit
      QC: EquiJoin[T, ?] :<: C#M): UnicoalesceEJ[T, C] = new UnicoalesceEJ[T, C] {

    def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]) =
      C.coalesceEJ[C#M](idPrism.get)
  }
}

sealed trait UnicoalesceTJ[T[_[_]], C <: CoM] extends Unicoalesce[T, C]

private[qscript] trait UnicoalesceTJLowPriorityImplicits {
  implicit def default[T[_[_]], C <: CoM]: UnicoalesceTJ[T, C] = new UnicoalesceTJ[T, C] {
    def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]) = κ(None)
  }
}

object UnicoalesceTJ extends UnicoalesceTJLowPriorityImplicits {

  implicit def member[T[_[_]], C <: CoM](
    implicit
      QC: ThetaJoin[T, ?] :<: C#M): UnicoalesceTJ[T, C] = new UnicoalesceTJ[T, C] {

    def apply(C: Coalesce.Aux[T, C#M, C#M])(implicit F: Functor[C#M]) =
      C.coalesceTJ[C#M](idPrism.get)
  }
}
