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

import quasar._, Predef._, fp._
import scala.collection.{ mutable => scm }
import scala.reflect.macros.blackbox._
import jawn.{ Facade, FContext }
import java.util.UUID
import scala.Any

class JsonMacroImpls(val c: Context) {
  import c.universe._

  type MacroFacade = Facade[Tree]
  type JMap        = Map[String, Tree]

  class TreeFacade(prefix: Tree, keyMap: JMap, valueMap: JMap) extends Facade[Tree] {
    def stringKey(s: String): Tree      = q"$s"
    def keyOrLiteral(k: String): Tree   = keyMap.getOrElse(k, stringKey(k))
    def valueOrLiteral(x: String): Tree = valueMap.getOrElse(x, jstring(x))

    def jtrue(): Tree                 = q"$prefix.jtrue"
    def jfalse(): Tree                = q"$prefix.jfalse"
    def jnull: Tree                   = q"$prefix.jnull"
    def jnum(s: String): Tree         = q"$prefix.jnum($s)"
    def jint(s: String): Tree         = q"$prefix.jint($s)"
    def jstring(s: String): Tree      = q"$prefix.jstring($s)"

    def singleContext(): FContext[Tree] = new FContext[Tree] {
      var value: Tree = _

      def add(v: Tree): Unit   = value = v
      def add(s: String): Unit = add(valueOrLiteral(s))
      def finish: Tree         = value
      def isObj: Boolean       = false
    }
    def arrayContext(): FContext[Tree] = new FContext[Tree] {
      val buf = List.newBuilder[Tree]

      def add(v: Tree): Unit   = ignore(buf += v)
      def add(s: String): Unit = add(valueOrLiteral(s))
      def isObj: Boolean       = false
      def finish: Tree         = q"""{
        val fcontext = $prefix.arrayContext()
        ${buf.result} foreach (fcontext add _)
        fcontext.finish
      }"""
    }
    def objectContext(): FContext[Tree] = new FContext[Tree] {
      var key: String = null
      val buf         = List.newBuilder[Tree -> Tree]

      private def pair(l: Tree, r: Tree): Tree -> Tree = l -> r
      private def clearKey[A](body: A): Unit   = ignore(key = null)

      def add(arg: String): Unit = key match {
        case null => key = arg
        case _    => clearKey(buf += (keyOrLiteral(key) -> valueOrLiteral(arg)))
      }
      def add(arg: Tree): Unit = clearKey(buf += (keyOrLiteral(key) -> arg))
      def isObj: Boolean       = true
      def finish: Tree         = q"""{
        val fcontext = $prefix.objectContext()
        for ((k, v) <- ${buf.result}) {
          fcontext add k
          fcontext add v
        }
        fcontext.finish
      }"""
    }
  }

  private def fail(msg: String): Nothing  = c.abort(c.enclosingPosition, msg)
  private def fail(t: Throwable): Nothing = fail("Exception during json interpolation: " + t.getMessage)

  object Args {
    def unapply(t: Tree) = t match {
      case Apply(Apply(qual, Apply(_, parts) :: Nil), implicitArg :: Nil) => Some((qual, parts, implicitArg))
      case _                                                              => None
    }
  }

  trait JsonMacroBase {
    type M[X]
    def parse[A: c.WeakTypeTag](json: String, facade: MacroFacade): c.Expr[M[A]]

    def apply[A: c.WeakTypeTag](args: c.Expr[Any]*): c.Expr[M[A]] = c.prefix.tree match {
      case Args(qual, parts, facade) =>
        val A      = weakTypeOf[A]

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
          values(uuid) = if (tpe =:= A) q"$arg" else q"quasar.JLift.liftJson[$tpe, $A]($arg)"

          if (tpe =:= typeOf[String])
            keys(uuid) = q"$arg"
        }

        if (stringParts.size != uuids.size + 1)
          fail("Invalid arguments to json interpolator")

        parse(
          ( for ((part, uuid) <- stringParts zip uuids) yield part + "\"" + uuid + "\"" ) mkString ("", "", stringParts.last),
          new TreeFacade(q"$facade", keys.toMap, values.toMap)
        )

      case tree =>
        fail("Unexpected tree shape for json interpolation macro: " + tree.getClass + "\n" + showRaw(tree))
    }
  }


  def singleImpl[A: c.WeakTypeTag](args: c.Expr[Any]*): c.Expr[A]       = JsonMacroSingle[A](args: _*)
  def manyImpl[A: c.WeakTypeTag](args: c.Expr[Any]*): c.Expr[Vector[A]] = JsonMacroMany[A](args: _*)

  object JsonMacroSingle extends JsonMacroBase {
    type M[X] = X
    def parse[A: c.WeakTypeTag](json: String, facade: MacroFacade): c.Expr[A] =
      c.Expr[A](JParser.parse(json)(facade).fold(fail, x => x))
  }
  object JsonMacroMany extends JsonMacroBase {
    type M[X] = Vector[X]
    def parse[A: c.WeakTypeTag](json: String, facade: MacroFacade): c.Expr[M[A]] =
      c.Expr[Vector[A]](JParser.parseMany(json + "\n")(facade).fold(fail, x => q"${x.toVector}"))
  }
}
