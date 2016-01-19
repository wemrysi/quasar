/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.fp

import quasar.Predef.{Array, String, SuppressWarnings}
import quasar.fs.APath

import argonaut._, Argonaut._
import pathy.Path, Path._

// TODO: These belong in argonaut sub-module in scala-pathy
object PathyCodecJson {
  implicit def pathEncodeJson[B,T]: EncodeJson[Path[B,T,Sandboxed]] =
    EncodeJson.of[String].contramap(posixCodec.printPath(_))

  implicit val aPathDecodeJson: DecodeJson[APath] =
    DecodeJson.of[String] flatMap (s => DecodeJson(hc =>
      posixCodec.parseAbsFile(s).orElse(posixCodec.parseAbsDir(s))
        .map(sandboxAbs)
        .fold(DecodeResult.fail[APath]("[T]AbsPath[T]", hc.history))(DecodeResult.ok)))

  // TODO: We know this can't fail, remove once Pathy is refactored to be more precise
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.OptionPartial"))
  private def sandboxAbs[T,S](p: Path[Abs,T,S]): Path[Abs,T,Sandboxed] =
    rootDir[Sandboxed] </> p.relativeTo(rootDir).get
}
