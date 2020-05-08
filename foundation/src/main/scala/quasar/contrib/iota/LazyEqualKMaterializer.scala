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

package quasar.contrib.iota

import slamdata.Predef._

import quasar.contrib.matryoshka.LazyEqual

import iotaz.TListK.:::
import iotaz.{CopK, TListK, TNilK}

import matryoshka.Delay

import scalaz._

sealed trait LazyEqualKMaterializer[LL <: TListK] {
  def materialize(offset: Int): Delay[LazyEqual, CopK[LL, ?]]
}

object LazyEqualKMaterializer {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def base[F[_]](
    implicit
    F: Delay[LazyEqual, F]
  ): LazyEqualKMaterializer[F ::: TNilK] = new LazyEqualKMaterializer[F ::: TNilK] {
    override def materialize(offset: Int): Delay[LazyEqual, CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      Delay.fromNT(new (LazyEqual ~> λ[a => LazyEqual[CopK[F ::: TNilK, a]]]) {
        override def apply[A](eql: LazyEqual[A]): LazyEqual[CopK[F ::: TNilK, A]] = {
          LazyEqual lazyEqual {
            case (I(left), I(right)) => F(eql).equal(left, right)
            case _ => Need(false)
          }
        }
      })
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
    implicit
    F: Delay[LazyEqual, F],
    LL: LazyEqualKMaterializer[LL]
  ): LazyEqualKMaterializer[F ::: LL] = new LazyEqualKMaterializer[F ::: LL] {
    override def materialize(offset: Int): Delay[LazyEqual, CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      Delay.fromNT(new (LazyEqual ~> λ[a => LazyEqual[CopK[F ::: LL, a]]]) {
        override def apply[A](eql: LazyEqual[A]): LazyEqual[CopK[F ::: LL, A]] = {
          LazyEqual lazyEqual {
            case (I(left), I(right)) => F(eql).equal(left, right)
            case (left, right) => LL.materialize(offset + 1)(eql).equal(left.asInstanceOf[CopK[LL, A]], right.asInstanceOf[CopK[LL, A]])
          }
        }
      })
    }
  }
}
