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

package quasar.impl.storage

import scala.annotation.implicitNotFound

import scodec._
import scodec.bits.BitVector

import shapeless._

/*
 * An evidence that it's possible to find a codec for every element of HList.
 * We use it to remove all possible incoherencies during encoding keys in prefix store.
 * The codec puts and takes bits linearly.
 */
@implicitNotFound("Could not find LinearCodec for ${L}.")
sealed trait LinearCodec[L <: HList] extends Codec[L]

object LinearCodec {
  def apply[L <: HList](implicit ev: LinearCodec[L]): LinearCodec[L] = ev

  implicit def singletonLinearCodec[H](implicit hCodec: Codec[H]): LinearCodec[H :: HNil] = new LinearCodec[H :: HNil] {
    def encode(a: H :: HNil): Attempt[BitVector] = a match {
      case h :: _ => hCodec.encode(h)
    }
    def decode(a: BitVector): Attempt[DecodeResult[H :: HNil]] = hCodec.decode(a).map(_.map(_ :: HNil))
    def sizeBound: SizeBound = hCodec.sizeBound
  }

  implicit def consLinearCodec[H, L <: HList](implicit hCodec: Codec[H], tailCodec: LinearCodec[L]): LinearCodec[H :: L] =
    new LinearCodec[H :: L] {
      def encode(a: H :: L): Attempt[BitVector] = a match {
        case h :: t => for {
          hBits <- hCodec.encode(h)
          tBits <- tailCodec.encode(t)
        } yield hBits ++ tBits
      }
      def decode(a: BitVector): Attempt[DecodeResult[H :: L]] = for {
        hResult <- hCodec.decode(a)
        tResult <- tailCodec.decode(hResult.remainder)
      } yield tResult.map(t => hResult.value :: t)

      def sizeBound = hCodec.sizeBound + tailCodec.sizeBound
    }
}
