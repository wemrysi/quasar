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

package quasar.yggdrasil.vfs

import org.specs2.mutable._

import java.nio.file.{Files, FileVisitOption, Path}
import java.io.File
import java.util.Comparator

object RealPOSIXSpecs extends Specification {

  "real posix interpreter" should {
    "mkdir on root if non-existent" in {
      val target = new File(newBase(), "non-existent")
      RealPOSIX(target).unsafePerformSync

      target.exists() mustEqual true
      target.isDirectory() mustEqual true
    }
  }

  def newBase() = Files.createTempDirectory("RealPOSIXSpecs").toFile
}
