package ygg

import jawn._
import blueeyes._
import blueeyes.json.JValue

package object json {
  type Result[A] = scalaz.Validation[Throwable, A]

  implicit class AsyncParserOps[A](p: AsyncParser[A])(implicit z: Facade[A]) {
    type R = AsyncParse[A] -> AsyncParser[A]

    def apply(s: String): R       = apply(utf8Bytes(s))
    def apply(bb: Array[Byte]): R = apply(ByteBufferWrap(bb))
    def apply(bb: ByteBuffer): R = p absorb bb match {
      case scala.util.Right(x)                => AsyncParse(Nil, x) -> p.copy()
      case scala.util.Left(t: ParseException) => AsyncParse(Seq(t), Nil) -> p.copy()
      case scala.util.Left(t)                 => AsyncParse(Seq(new ParseException(t.getMessage, 0, 0, 0)), Nil) -> p.copy()
    }
  }

  implicit val YggFacade: SimpleFacade[JValue] = new SimpleFacade[JValue] {
    import blueeyes.json._

    def jnull()                          = JNull
    def jfalse()                         = JFalse
    def jtrue()                          = JTrue
    def jnum(s: String)                  = JNum(s)
    def jint(s: String)                  = JNum(s)
    def jstring(s: String)               = JString(s)
    def jarray(vs: List[JValue])         = JArray(vs)
    def jobject(vs: Map[String, JValue]) = JObject(vs)
  }

  implicit def tryToResult[A](x: Try[A]): Result[A] = x match {
    case scala.util.Success(x) => scalaz.Success(x)
    case scala.util.Failure(t) => scalaz.Failure(t)
  }
  implicit def eitherToResult[L, R](x: Either[L, R]): Result[R] = x match {
    case scala.util.Right(x)           => scalaz.Success(x)
    case scala.util.Left(t: Throwable) => scalaz.Failure(t)
    case scala.util.Left(x)            => scalaz.Failure(new RuntimeException("" + x))
  }
}
