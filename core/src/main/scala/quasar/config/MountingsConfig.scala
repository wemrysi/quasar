/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.config

import slamdata.Predef._
import quasar.fs.mount

import argonaut._, Argonaut._
import monocle.macros.Lenses
import scalaz.concurrent.Task

@Lenses final case class MountingsConfig(mountings: Option[mount.MountingsConfig])

object MountingsConfig {
  val fieldName = "mountings"

  implicit val configOps: ConfigOps[MountingsConfig] = new ConfigOps[MountingsConfig] {
    val name = "mountings"

    val default = Task.now(MountingsConfig(None))
  }

  implicit val codec: CodecJson[MountingsConfig] =
    casecodec1(MountingsConfig.apply, MountingsConfig.unapply)(fieldName)
}
