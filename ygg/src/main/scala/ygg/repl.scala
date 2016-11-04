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

package ygg

import common._, macros._
import json._, table._, trans.{ TransSpec => _, _ }
import Table._

object repl extends FacadeBasedInterpolator[JValue] {
  def medals = fromJson(jsonMany"""
    {"key":[5908438637678314371],"value":{"Edition":"2000","Gender":"Men"}}
    {"key":[5908438637678314372],"value":{"Edition":"1996","Gender":"Men"}}
    {"key":[5908438637678314373],"value":{"Edition":"2008","Gender":"Men"}}
    {"key":[5908438637678314374],"value":{"Edition":"2004","Gender":"Women"}}
    {"key":[5908438637678314375],"value":{"Edition":"2000","Gender":"Women"}}
    {"key":[5908438637678314376],"value":{"Edition":"1996","Gender":"Women"}}
    {"key":[5908438637678314377],"value":{"Edition":"2008","Gender":"Men"}}
    {"key":[5908438637678314378],"value":{"Edition":"2004","Gender":"Men"}}
    {"key":[5908438637678314379],"value":{"Edition":"1996","Gender":"Men"}}
    {"key":[5908438637678314380],"value":{"Edition":"2008","Gender":"Women"}}
  """)

  implicit class TableSelectionOps(val table: Table) {
    def dump(): Unit                             = table.toVector foreach println
    def p(): Unit                                = dump()

    def map(f: TransSpec1): Table                = table transform f
    def filter(p: TransSpec[Source.type]): Table = map(root filter p)

    def \(path: JPath): Table  = path.nodes.foldLeft(table)(_ \ _)
    def \(path: String): Table = this \ JPath(path)
    def \(node: JPathNode): Table = node match {
      case JPathField(name) => map(root select name)
      case JPathIndex(idx)  => map(root apply idx)
    }
  }
}
