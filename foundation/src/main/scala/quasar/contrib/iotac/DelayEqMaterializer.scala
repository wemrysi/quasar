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

package quasar.contrib.iotac

import slamdata.Predef.{Eq => _, _}

import _root_.iota.{CopK, TListK, TNilK}, TListK.:::
import cats.{~>, Eq}
import higherkindness.droste.Delay

sealed trait DelayEqMaterializer[LL <: TListK] {
  def materialize(offset: Int): Delay[Eq, CopK[LL, ?]]
}

object DelayEqMaterializer {
  implicit def base[F[_]](implicit F: Delay[Eq, F]): DelayEqMaterializer[F ::: TNilK] = new DelayEqMaterializer[F ::: TNilK] {
    def materialize(offset: Int): Delay[Eq, CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      new (Eq ~> λ[α => Eq[CopK[F ::: TNilK, α]]]) {
        def apply[A](eqa: Eq[A]): Eq[CopK[F ::: TNilK, A]] = new Eq[CopK[F ::: TNilK, A]] {
          def eqv(a: CopK[F ::: TNilK, A], b: CopK[F ::: TNilK, A]): Boolean = (a, b) match {
            case (I(left), I(right)) => F(eqa).eqv(left, right)
            case _ => false
          }
        }
      }
    }
  }
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
      implicit F: Delay[Eq, F], LL: DelayEqMaterializer[LL])
      : DelayEqMaterializer[F ::: LL] = new DelayEqMaterializer[F ::: LL] {
    def materialize(offset: Int): Delay[Eq, CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      new (Eq ~> λ[α => Eq[CopK[F ::: LL, α]]]) {
        def apply[A](eqa: Eq[A]): Eq[CopK[F ::: LL, A]] = new Eq[CopK[F ::: LL, A]] {
          def eqv(a: CopK[F ::: LL, A], b: CopK[F ::: LL, A]) = (a, b) match {
            case (I(left), I(right)) => F(eqa).eqv(left, right)
            case (left, right) => LL.materialize(offset + 1)(eqa).eqv(
              left.asInstanceOf[CopK[LL, A]],
              right.asInstanceOf[CopK[LL, A]])
          }
        }
      }
    }
  }
}
