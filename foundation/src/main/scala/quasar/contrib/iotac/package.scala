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

package quasar.contrib

import slamdata.Predef.{Eq => _, _}

import _root_.cats.{Eq, Functor, Traverse}
import _root_.iota.{CopK, TListK}
import higherkindness.droste.Delay

package object iotac {
  type ACopK[α] = CopK[_, α]
  type :<<:[F[_], G[α] <: ACopK[α]] = CopK.Inject[F, G]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def computeListK[LL <: TListK, RR <: TListK, A](inp: CopK[LL, A])(
      implicit compute: TListK.Compute.Aux[LL, RR])
      : CopK[RR, A] = {
    val _ = compute
    inp.asInstanceOf[CopK[RR, A]]
  }

  def mkInject[F[_], LL <: TListK](i: Int): CopK.Inject[F, CopK[LL, ?]] =
    CopK.Inject.injectFromInjectL[F, LL](
      CopK.InjectL.makeInjectL[F, LL](
        new TListK.Pos[LL, F] { val index = i }))

  implicit def copkFunctor[LL <: TListK](implicit M: FunctorMaterializer[LL]): Functor[CopK[LL, ?]] =
    M.materialize(offset = 0)
  implicit def copkDelayEq[LL <: TListK](implicit M: DelayEqMaterializer[LL]): Delay[Eq, CopK[LL, ?]] =
    M.materialize(offset = 0)
  implicit def copkTraverse[LL <: TListK](implicit M: TraverseMaterializer[LL]): Traverse[CopK[LL, ?]] =
    M.materialize(offset = 0)
}
