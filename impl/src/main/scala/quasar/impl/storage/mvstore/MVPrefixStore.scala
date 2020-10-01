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

package quasar.impl.storage.mvstore

import slamdata.Predef._

import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.{CodecPrefixStore, PrefixStore, StoreError}

import scala.collection.JavaConverters._
import java.util.{Map => JMap}

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._

import fs2.Stream

import org.h2.mvstore._

import shapeless._
import shapeless.ops.hlist._
import shapeless.ops.traversable._

import scodec.codecs._
import scodec.bits.BitVector
import scodec._

object MVPrefixStore {
  def apply[F[_]: MonadError_[?[_], StoreError]: Sync: ContextShift, K <: HList: Codec, V: Codec](
      db: MVStore,
      name: String,
      blocker: Blocker)
      : F[PrefixStore.SCodec[F, K, V]] =
    MVPrefixableStore[F, Byte, Array[Byte]](db, name, blocker).map(CodecPrefixStore[F, K, V](_))
}
