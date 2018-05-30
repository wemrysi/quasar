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

package quasar.contrib.fs2

import slamdata.Predef.{Int, None, Some, Vector}

import fs2.Chunk
import scalaz.{Applicative, Foldable, Traverse}

trait ChunkInstances {
  implicit val chunkTraverse: Traverse[Chunk] =
    new Traverse[Chunk] with Foldable.FromFoldr[Chunk] {
      def traverseImpl[F[_], A, B](fa: Chunk[A])(f: A => F[B])(implicit F: Applicative[F]) =
        F.map(fa.foldLeft(F.point(Vector.empty[B])) { (fvb, a) =>
            F.apply2(fvb, f(a))(_ :+ _)
        })(Chunk.indexedSeq)

      override def foldRight[A, B](fa: Chunk[A], z: => B)(f: (A, => B) => B) =
        fa.foldRight(z)((a, b) => f(a, b))

      override def foldLeft[A, B](fa: Chunk[A], z: B)(f: (B, A) => B) =
        fa.foldLeft(z)(f)

      override def length[A](fa: Chunk[A]) =
        fa.size

      override def index[A](fa: Chunk[A], i: Int) =
        if (i >= 0 && i < fa.size)
          Some(fa(i))
        else
          None

      override def empty[A](fa: Chunk[A]) =
        fa.isEmpty
    }
}

object chunk extends ChunkInstances
