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

import quasar.Predef._
import scala.util.Sorting.quickSort
import scala.annotation.switch
import JValue._
import java.lang.StringBuilder
import scala.{ Iterator }

/** Adapted from jawn 0.9.0. */
sealed trait Renderer {
  final def render(jv: JValue): String = {
    val sb = new StringBuilder
    render(sb, 0, jv)
    sb.toString
  }

  final def render(sb: StringBuilder, depth: Int, jv: JValue): Unit = {
    jv match {
      case JUndefined  => sb append "<undef>"
      case JNull       => sb.append("null")
      case JTrue       => sb.append("true")
      case JFalse      => sb.append("false")
      case JNum(x)     => sb append x.toString
      case JString(s)  => renderString(sb, s)
      case JArray(vs)  => renderArray(sb, depth, vs)
      case JObject(vs) => renderObject(sb, depth, canonicalizeObject(vs))
    }
    ()
  }

  def canonicalizeObject(vs: Map[String, JValue]): Iterator[JStringValue]

  def renderString(sb: StringBuilder, s: String): Unit

  final def renderArray(sb: StringBuilder, depth: Int, vs: Seq[JValue]): Unit = {
    if (vs.isEmpty) return { sb.append("[]"); () }
    sb.append("[")
    render(sb, depth + 1, vs(0))
    var i = 1
    while (i < vs.length) {
      sb.append(",")
      render(sb, depth + 1, vs(i))
      i += 1
    }
    sb.append("]")
    ()
  }

  final def renderObject(sb: StringBuilder, depth: Int, it: Iterator[JStringValue]): Unit = {
    if (!it.hasNext) return { sb.append("{}"); () }
    val (k0, v0) = it.next
    sb.append("{")
    renderString(sb, k0)
    sb.append(":")
    render(sb, depth + 1, v0)
    while (it.hasNext) {
      val (k, v) = it.next
      sb.append(",")
      renderString(sb, k)
      sb.append(":")
      render(sb, depth + 1, v)
    }
    sb.append("}")
    ()
  }

  final def escape(sb: StringBuilder, s: String, unicode: Boolean): Unit = {
    sb.append('"')
    var i = 0
    val len = s.length
    while (i < len) {
      (s.charAt(i): @switch) match {
        case '"' => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case c =>
          if (c < ' ' || (c > '~' && unicode)) sb.append("\\u%04x" format c.toInt)
          else sb.append(c)
      }
      i += 1
    }
    sb.append('"')
    ()
  }
}

object CanonicalRenderer extends Renderer {
  def canonicalizeObject(vs: Map[String, JValue]): Iterator[JStringValue] = {
    val keys = vs.keys.toArray
    quickSort(keys)
    keys.iterator.map(k => (k, vs(k)))
  }
  def renderString(sb: StringBuilder, s: String): Unit =
    escape(sb, s, true)
}

object FastRenderer extends Renderer {
  def canonicalizeObject(vs: Map[String, JValue]): Iterator[JStringValue] = vs.iterator
  def renderString(sb: StringBuilder, s: String): Unit                    = escape(sb, s, false)
}
