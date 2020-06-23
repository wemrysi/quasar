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

import slamdata.Predef._

import _root_.iota.{CopK, TListK, TNilK}, TListK.:::
import cats.{Applicative, Traverse}
import cats.implicits._
import higherkindness.droste.util.DefaultTraverse

sealed trait TraverseMaterializer[LL <: TListK] {
  def materialize(offset: Int): Traverse[CopK[LL, ?]]
}

object TraverseMaterializer {
  implicit def base[F[_]: Traverse]: TraverseMaterializer[F ::: TNilK] = new TraverseMaterializer[F ::: TNilK] {
    def materialize(offset: Int): Traverse[CopK[F ::: TNilK, ?]] = {
      val I = mkInject[F, F ::: TNilK](offset)
      new DefaultTraverse[CopK[F ::: TNilK, ?]] {
        def traverse[G[_]: Applicative, A, B](cfa: CopK[F ::: TNilK, A])(f: A => G[B]): G[CopK[F ::: TNilK, B]] = cfa match {
          case I(fa) => fa.traverse(f).map(I(_))
        }
      }
    }
  }
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def induct[F[_]: Traverse, LL <: TListK](
      implicit LL: TraverseMaterializer[LL])
      : TraverseMaterializer[F ::: LL] = new TraverseMaterializer[F ::: LL] {
    def materialize(offset: Int): Traverse[CopK[F ::: LL, ?]] = {
      val I = mkInject[F, F ::: LL](offset)
      new DefaultTraverse[CopK[F ::: LL, ?]] {
        def traverse[G[_]: Applicative, A, B](cfa: CopK[F ::: LL, A])(f: A => G[B]): G[CopK[F ::: LL, B]] = cfa match {
          case I(fa) => fa.traverse(f).map(I(_))
          case other => LL.materialize(offset + 1).traverse(other.asInstanceOf[CopK[LL, A]])(f).asInstanceOf[G[CopK[F ::: LL, B]]]
        }
      }
    }
  }
}
