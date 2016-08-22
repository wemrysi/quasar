package ygg.macros

import scala.collection.{ mutable => scm }
import scala.reflect.macros.whitebox
import jawn._, Parser.parseUnsafe

class JsonMacros(val c: whitebox.Context) {
  import c.universe._

  private def fail(msg: String): Nothing = c.abort(c.enclosingPosition, msg)

  private def freshUUID(exclude: Seq[String]): String = java.util.UUID.randomUUID.toString match {
    case uuid if exclude forall (x => !(x contains uuid)) => uuid
    case _                                                => freshUUID(exclude)
  }

  final def jsonImpl(args: c.Expr[Any]*): Tree = c.prefix.tree match {
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
        val uuid     = freshUUID(stringParts)
        uuids        = uuids :+ uuid
        values(uuid) = q"io.circe.Encoder[$tpe].apply($arg)"

        if (tpe =:= typeOf[String])
          keys(uuid) = q"$arg"
      }
      if (stringParts.size != uuids.size + 1)
        fail("Invalid arguments to json interpolator")

      val json = (stringParts, uuids).zipped map ((part, uuid) => part + "\"" + uuid + "\"") mkString ("", "", stringParts.last)

      scala.util.Try(parseUnsafe(json)(new MacroFacade(keys.toMap, values.toMap))) match {
        case scala.util.Success(t) => t
        case scala.util.Failure(t) => fail("Invalid JSON in interpolated string: " + t.getMessage)
      }

    case tree => fail("Unexpected tree shape for json interpolation macro: " + tree)
  }

  class MacroFacade(keys: Map[String, Tree], values: Map[String, Tree]) extends Facade[Tree] {
    def jnull: Tree                          = q"ygg.json.JNull"
    def jfalse: Tree                         = q"ygg.json.JFalse"
    def jtrue: Tree                          = q"ygg.json.JTrue"
    def jnum(s: String): Tree                = q"ygg.json.JNum($s)"
    def jint(s: String): Tree                = q"ygg.json.JNum($s)"
    def jstring(s: String): Tree             = q"ygg.json.JString($s)"
    def jarray(xs: Array[Tree]): Tree        = q"ygg.json.JArray(${xs.toList})"
    def jobject(xs: List[Tree]): Tree        = q"ygg.json.jobject($xs: _*)"
    def jfield(key: Tree, value: Tree): Tree = q"ygg.json.JField($key, $value)"

    def toJsonKey(s: String): Tree     = keys.getOrElse(s, q"$s")
    def toJsonString(s: String): Tree  = values.getOrElse(s, jstring(s))

    def singleContext(): FContext[Tree] = new FContext[Tree] {
      var value: Tree = _

      def add(s: String): Unit = value = toJsonString(s)
      def add(v: Tree): Unit   = value = v
      def finish: Tree         = value
      def isObj: Boolean       = false
    }

    final def arrayContext(): FContext[Tree] = new FContext[Tree] {
      val vs = scm.ArrayBuffer[Tree]()

      def add(s: String): Unit = vs append toJsonString(s)
      def add(v: Tree): Unit   = vs append v
      def finish: Tree         = jarray(vs.toArray)
      def isObj: Boolean       = false
    }
    final def objectContext(): FContext[Tree] = new FContext[Tree] {
      var key: String = null
      val fields      = scm.ArrayBuffer[Tree]()

      private def clearKey(body: Unit): Unit = key = null
      def add(arg: String): Unit = key match {
        case null => key = arg
        case _    => clearKey( fields append jfield(toJsonKey(key), toJsonString(arg)) )
      }
      def add(arg: Tree): Unit = clearKey( fields append jfield(toJsonKey(key), arg) )
      def finish: Tree         = jobject(fields.toList)
      def isObj: Boolean       = true
    }
  }
}
