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

package quasar.impl.datasources

import quasar.impl.storage
import quasar.impl.storage.mvstore.MVStorePrefixStore
//import quasar.impl.storage.mapdb.MapDbPrefixStore

import slamdata.Predef._
import scala.Predef.classOf

import cats.Eq
import cats.effect.{Blocker, IO, Resource}
import cats.implicits._

import org.mapdb.{DBMaker, Serializer}

import java.lang.Integer

import scala.concurrent.ExecutionContext.Implicits.global

import shapeless._

import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.{int32, bytes, utf8_32, variableSizeBytes}

import PrefixByteStoresSpec._

final class PrefixByteStoresSpec extends ByteStoresSpec[IO, Int] {
  implicit val intCodec: Codec[Int] = int32

  implicit val strCodec: Codec[String] = utf8_32

  implicit val arrayByteCodec: Codec[Array[Byte]] =
    variableSizeBytes(int32, bytes).xmapc(_.toArray)(ByteVector(_))

  val byteStores =
    storage.offheapMVStore[IO] evalMap { db =>
//    Resource.make(IO(DBMaker.memoryDB().make()))(db => IO(db.close())) evalMap { db =>
      val prefixStore =
        MVStorePrefixStore[IO, Int :: String :: HNil, Array[Byte]](
          db,
          "prefix-bytestores-spec",
          Blocker.liftExecutionContext(global))
//        MapDbPrefixStore[IO](
//          "prefix-bytestores-spec",
//          db,
//          Serializer.INTEGER :: Serializer.STRING :: HNil,
//          Serializer.BYTE_ARRAY,
//          Blocker.liftExecutionContext(global))

      prefixStore.map(PrefixByteStores(_))
    }

  val k1 = 3
  val k2 = 7
}

object PrefixByteStoresSpec {
//  implicit val jIntegerEq: Eq[Integer] =
//    Eq.by(_.intValue)
}
