/*
 * Copyright 2014–2016 SlamData Inc.
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

package ygg.json

import ygg.common._

final case class JPath(nodes: Vec[JPathNode]) extends ToString {
  def to_s: String = nodes match {
    case Vec() => "."
    case _     => nodes mkString ""
  }
}

sealed abstract class JPathNode(val to_s: String) extends ToString
final case class JPathField(name: String)         extends JPathNode("." + name)
final case class JPathIndex(index: Int)           extends JPathNode(s"[$index]")

object JPath {
  private val PathPattern  = """[.]|(?=\[\d+\])""".r
  private val IndexPattern = """^\[(\d+)\]$""".r
  private def ppath(p: String) = if (p startsWith ".") p else "." + p

  def apply(xs: List[JPathNode]): JPath = new JPath(xs.toVector)
  def apply(n: JPathNode*): JPath = new JPath(n.toVector)
  def apply(path: String): JPath = JPath(
    PathPattern split ppath(path) map (_.trim) flatMap {
      case ""                  => Vec()
      case IndexPattern(index) => Vec(JPathIndex(index.toInt))
      case name                => Vec(JPathField(name))
    } toVector
  )
}
