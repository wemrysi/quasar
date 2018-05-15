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

package quasar.precog.common

class Path private (val elements: String*) {
  val path: String = elements.mkString("/", "/", "/").replaceAll("/+", "/")

  override def equals(that: Any) = that match {
    case Path(`path`) => true
    case _            => false
  }

  override def hashCode = path.hashCode
  override def toString = path
}

object Path {
  val Root = new Path()

  private def cleanPath(string: String): String = string.replaceAll("^/|/$", "").replaceAll("/+", "/")

  def apply(path: String): Path = new Path(cleanPath(path).split("/").filterNot(_.trim.isEmpty): _*)

  def unapply(path: Path): Option[String] = Some(path.path)
}
