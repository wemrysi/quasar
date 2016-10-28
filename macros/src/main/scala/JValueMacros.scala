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

package ygg.macros

import quasar.Predef._
import scala.collection.{ mutable => scm }
import scala.reflect.macros.blackbox._
import jawn._
import java.util.UUID
import scala.Any
import scalaz.{ Tree => _, _ }, Scalaz._

class JValueMacros(val c: Context) {
  import c.universe._

  def jsonInterpolatorImpl(args: c.Expr[Any]*): Tree     = JsonMacroSingle(args: _*)
  def jsonManyInterpolatorImpl(args: c.Expr[Any]*): Tree = JsonMacroMany(args: _*)

  private def fail(msg: String): Nothing  = c.abort(c.enclosingPosition, msg)
  private def fail(t: Throwable): Nothing = fail("Exception during json interpolation: " + t.getMessage)

  trait JsonMacroBase {
    def parse(json: String, facade: Facade[Tree]): Tree

    def apply(args: c.Expr[Any]*): Tree = c.prefix.tree match {
      case Apply(_, Apply(_, parts) :: Nil) =>
        val stringParts = parts map {
          case Literal(Constant(part: String)) => part
          case _                               => fail("A StringContext part for the json interpolator is not a string")
        }
        var uuids  = Vector[String]()
        val keys   = scm.Map[String, Tree]()
        val values = scm.Map[String, Tree]()

        args foreach { arg =>
          val tpe      = c.typecheck(arg.tree).tpe
          val uuid     = UUID.randomUUID.toString
          uuids        = uuids :+ uuid
          values(uuid) = q"quasar.JEncoder.lift[$tpe, ygg.json.JValue]($arg)"

          if (tpe =:= typeOf[String])
            keys(uuid) = q"$arg"
        }

        if (stringParts.size != uuids.size + 1)
          fail("Invalid arguments to json interpolator")

        parse(
          ( for ((part, uuid) <- stringParts zip uuids) yield part + "\"" + uuid + "\"" ) mkString ("", "", stringParts.last),
          facades.create(keys.toMap, values.toMap)
        )

      case tree => fail("Unexpected tree shape for json interpolation macro: " + tree)
    }
  }

  object JsonMacroSingle extends JsonMacroBase {
    def parse(json: String, facade: Facade[Tree]): Tree =
      JParser.parse(json)(facade).fold(fail, x => x)
  }
  object JsonMacroMany extends JsonMacroBase {
    def parse(json: String, facade: Facade[Tree]): Tree =
      JParser.parseMany(json + "\n")(facade).fold(fail, x => q"${x.toVector}")
  }

  object facades extends TreeFacades[c.type](c) {
    type M[X] = List[X]

    def create(): TreeFacade                                                   = create(Map(), Map())
    def create(keys: Map[String, Tree], values: Map[String, Tree]): TreeFacade = new TreeFacade(keys, values)

    def stringKey(s: String): Tree = q"$s"
    def newBuilder[A]() = new Builder[A] {
      val buf            = scm.ListBuffer[A]()
      def +=(x: A): Unit = discard(buf += x)
      def result: M[A]   = buf.result
    }

    def jbool(x: Boolean)                    = if (x) q"ygg.json.JTrue" else q"ygg.json.JFalse"
    def jnull: Tree                          = q"ygg.json.JNull"
    def jnum(s: String): Tree                = q"ygg.json.JNum($s)"
    def jint(s: String): Tree                = q"ygg.json.JNum($s)"
    def jstring(s: String): Tree             = q"ygg.json.JString($s)"
    def jarray(xs: List[Tree]): Tree         = q"ygg.json.JArray($xs)"
    def jobject(xs: List[Tree]): Tree        = q"ygg.json.JObject($xs)"
    def jfield(key: Tree, value: Tree): Tree = q"ygg.json.JField($key, $value)"
  }
}

abstract class TreeFacades[C <: Context](val c: C) {
  outer =>

  import c.universe._

  type M[X]
  type JField = (Tree, Tree)

  trait Builder[A] {
    def +=(x: A): Unit
    def result: M[A]
  }
  def newBuilder[A](): Builder[A]
  def stringKey(s: String): Tree

  def jnull: Tree
  def jbool(x: Boolean): Tree
  def jnum(s: String): Tree
  def jint(s: String): Tree
  def jstring(s: String): Tree
  def jarray(xs: M[Tree]): Tree
  def jobject(xs: M[Tree]): Tree
  def jfield(key: Tree, value: Tree): Tree

  class TreeFacade(keyMap: Map[String, Tree], valueMap: Map[String, Tree]) extends Facade[Tree] {
    def keyOrLiteral(k: String): Tree   = keyMap.getOrElse(k, stringKey(k))
    def valueOrLiteral(x: String): Tree = valueMap.getOrElse(x, jstring(x))

    def jnull: Tree                = outer.jnull
    def jfalse: Tree               = jbool(false)
    def jtrue: Tree                = jbool(true)
    def jnum(s: String): Tree      = outer.jnum(s)
    def jint(s: String): Tree      = outer.jint(s)
    def jstring(s: String): Tree   = outer.jstring(s)
    def jarray(xs: M[Tree]): Tree  = outer.jarray(xs)
    def jobject(xs: M[Tree]): Tree = outer.jobject(xs)

    def singleContext(): FContext[Tree] = new FContext[Tree] {
      var value: Tree = _

      def add(v: Tree): Unit   = value = v
      def add(s: String): Unit = add(valueOrLiteral(s))
      def finish: Tree         = value
      def isObj: Boolean       = false
    }

    def arrayContext(): FContext[Tree] = new FContext[Tree] {
      val buf = newBuilder[Tree]()

      def add(v: Tree): Unit   = buf += v
      def add(s: String): Unit = add(valueOrLiteral(s))
      def finish: Tree         = jarray(buf.result)
      def isObj: Boolean       = false
    }
    def objectContext(): FContext[Tree] = new FContext[Tree] {
      var key: String = null
      val buf         = newBuilder[Tree]()

      private def clearKey(body: Unit): Unit = key = null
      def add(arg: String): Unit = key match {
        case null => key = arg
        case _    => clearKey( buf += jfield(keyOrLiteral(key), valueOrLiteral(arg)) )
      }
      def add(arg: Tree): Unit = clearKey( buf += jfield(keyOrLiteral(key), arg) )
      def finish: Tree         = jobject(buf.result)
      def isObj: Boolean       = true
    }
  }
}
