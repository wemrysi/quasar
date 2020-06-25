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

package quasar

import slamdata.Predef._

import _root_.iota.{CopK, TListK, TNilK}, TListK.:::
import cats.~>
import higherkindness.droste.Delay
import quasar.contrib.iotac.mkInject

sealed trait DelayRenderTreeMaterlializer[LL <: TListK] {
  def materialize(offset: Int): Delay[RenderTree, CopK[LL, ?]]
}

object DelayRenderTreeMaterlializer {
  implicit def base[F[_]](implicit F: Delay[RenderTree, F]): DelayRenderTreeMaterlializer[F ::: TNilK] =
    new DelayRenderTreeMaterlializer[F ::: TNilK] {
      def materialize(offset: Int): Delay[RenderTree, CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new (RenderTree ~> λ[a => RenderTree[CopK[F ::: TNilK, a]]]) {
          def apply[A](rt: RenderTree[A]): RenderTree[CopK[F ::: TNilK, A]] = {
            RenderTree make {
              case I(fa) => F(rt).render(fa)
            }
          }
        }
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_], LL <: TListK](
      implicit
      F: Delay[RenderTree, F],
      LL: DelayRenderTreeMaterlializer[LL])
      : DelayRenderTreeMaterlializer[F ::: LL] = new DelayRenderTreeMaterlializer[F ::: LL] {
    def materialize(offset: Int): Delay[RenderTree, CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      new (RenderTree ~> λ[a => RenderTree[CopK[F ::: LL, a]]]) {
        def apply[A](rt: RenderTree[A]): RenderTree[CopK[F ::: LL, A]] = {
          RenderTree make {
            case I(fa) => F(rt).render(fa)
            case other => LL.materialize(offset + 1)(rt).render(other.asInstanceOf[CopK[LL, A]])
          }
        }
      }
    }
  }
}
