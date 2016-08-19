package ygg.tests

import blueeyes._, json._
import ygg.json._
import java.net.URLDecoder
import scala.util.control.Exception._
import scalaz._
import JsonTestSupport._

class JsonParserSpec extends quasar.Qspec {
  import JParser._

  "Any valid json can be parsed" in {
    val parsing = (json: JValue) => { parseUnsafe(json.renderPretty); true }
    prop(parsing)
  }

  "Parsing is thread safe" in {
    import java.util.concurrent._

    val json     = Examples.person
    val executor = Executors.newFixedThreadPool(100)
    val results  = (0 to 100).map(_ => executor.submit(new Callable[JValue] { def call = parseUnsafe(json) })).toList.map(_.get)
    results.zip(results.tail).forall(pair => pair._1 == pair._2) mustEqual true
  }

  "All valid string escape characters can be parsed" in {
    parseUnsafe("[\"abc\\\"\\\\\\/\\b\\f\\n\\r\\t\\u00a0\\uffff\"]") must_== JArray(JString("abc\"\\/\b\f\n\r\t\u00a0\uffff") :: Nil)
  }
}

class ParserBugsSpec extends quasar.Qspec {
  "Unicode ffff is a valid char in string literal" in {
    JParser.parseFromString(""" {"x":"\uffff"} """) must not(throwAn[java.lang.Exception])
  }

  "Does not hang when parsing 2.2250738585072012e-308" in {
    allCatch.opt(JParser.parseFromString(""" [ 2.2250738585072012e-308 ] """)) mustNotEqual None
  }

  "Does not hang when parsing 22.250738585072012e-309" in {
    allCatch.opt(JParser.parseFromString(""" [ 22.250738585072012e-309 ] """)) mustNotEqual None
  }

  "Can parse funky characters" in {
    JParser.parseUnsafe(URLDecoder.decode("\"%E2%84%A2\"", "UTF-8")) must_== JString("™")
  }
}

class ParsingByteBufferSpec extends quasar.Qspec {
  "Respects current ByteBuffer's position" in {
    val bb = ByteBufferWrap(Array(54, 55, 56, 57))
    bb.remaining must_== 4
    bb.get must_== 54
    bb.remaining must_== 3
    JParser.parseFromByteBuffer(bb) must_== Success(JNum(789))
    bb.remaining must_== 0
  }

  "Respects current ByteBuffer's limit" in {
    val bb = ByteBufferWrap(Array(54, 55, 56, 57))
    bb.limit(3)
    JParser.parseFromByteBuffer(bb) must_== Success(JNum(678))
    bb.remaining must_== 0
    bb.limit(4)
    bb.remaining must_== 1
    JParser.parseFromByteBuffer(bb) must_== Success(JNum(9))
    bb.remaining must_== 0
  }
}

class AsyncParserSpec extends quasar.Qspec {
  "Handles whitespace correctly" in {
    def ja(ns: Int*) = JArray(ns.map(n => JNum(n)): _*)

    JParser.parseFromString("[1, 2,\t3,\n4,\r5]\r").toOption must_== Some(ja(1, 2, 3, 4, 5))
    JParser.parseManyFromString("[1,\r\n2]\r\n[3,\r\n4]\r\n").toOption must_== Some(Seq(ja(1, 2), ja(3, 4)))
    JParser.parseFromString("[1, 2,\t3,\n4,\u0000 5]").toOption must_== None
    JParser.parseManyFromString("[1,\r\n2]\u0000[3,\r\n4]\r\n").toOption must_== None
  }

  "Handles whitespace correctly" in {
    def ja(ns: Int*) = JArray(ns.map(n => JNum(n)): _*)

    JParser.parseFromString("[1, 2,\t3,\n4,\r5]\r").toOption must_== Some(ja(1, 2, 3, 4, 5))
    JParser.parseManyFromString("[1,\r\n2]\r\n[3,\r\n4]\r\n").toOption must_== Some(Seq(ja(1, 2), ja(3, 4)))
    JParser.parseFromString("[1, 2,\t3,\n4,\u0000 5]").toOption must_== None
    JParser.parseManyFromString("[1,\r\n2]\u0000[3,\r\n4]\r\n").toOption must_== None
  }
}

class ArrayUnwrappingSpec extends quasar.Qspec {
  "Unwrapping array parser treats non-arrays correctly" in {
    val p1                      = AsyncParser.unwrap()
    val (AsyncParse(e1, _), p2) = p1("""{"a": 1, "b": 2""")
    e1.length must_== 0

    // ending the object is valid
    val (AsyncParse(e2a, _), _) = p2("""}""")
    e2a.length must_== 0

    // acting like you're in an array is not valid
    val (AsyncParse(e2b, _), _) = p2("""}, 999""")
    e2b.length must_== 1

    // in unwrap mode only a single object is allowed
    val (AsyncParse(e2c, _), _) = p2("""} 999""")
    e2c.length must_== 1
  }

  "Unwrapping array parser performs adequately" in {
    val num = 100 * 1000
    val elem = """{"a": 999, "b": [1,2,3], "c": "fooooo", "d": {"aa": 123}}"""

    def sync(elem: String, num: Int): (Int, Int, Long) = {
      val sb = new StringBuilder
      sb.append("[" + elem)
      for (_ <- 1 until num) {
        sb.append(",")
        sb.append(elem)
      }
      sb.append("]")
      val data = sb.toString.getBytes("UTF-8")
      val bb   = ByteBufferWrap(data)
      val t0   = System.currentTimeMillis
      JParser.parseFromByteBuffer(bb)
      val ms = System.currentTimeMillis() - t0
      (1, data.length, ms)
    }

    def syncStream(elem: String, num: Int): (Int, Int, Long) = {
      val sb = new StringBuilder
      for (_ <- 0 until num) {
        sb.append(elem)
        sb.append("\n")
      }
      val data        = sb.toString.getBytes("UTF-8")
      val bb          = ByteBufferWrap(data)
      val t0          = System.currentTimeMillis
      val Success(js) = JParser.parseManyFromByteBuffer(bb)
      val ms          = System.currentTimeMillis() - t0
      (js.length, data.length, ms)
    }

    def async(parser: jawn.AsyncParser[JValue], isArray: Boolean, elem: String, num: Int): (Int, Int, Long) = {
      val elemsPerChunk    = 4520
      val completeChunks   = num / elemsPerChunk
      val partialChunkSize = num % elemsPerChunk
      assert(partialChunkSize != 0)

      val es       = (0 until elemsPerChunk).map(_ => elem)
      val leftover = (0 until partialChunkSize).map(_ => elem)
      val (firstChunk, chunk, lastChunk) = (
        if (isArray)
          (( es mkString ("[", ",", ","), es mkString ("", ",", ","), leftover mkString ("", ",", "]") ))
        else
          (( es mkString ("", "\n", "\n"), es mkString ("", "\n", "\n"), leftover mkString ("", "\n", "\n") ))
      )

      var i     = 0
      var p     = parser
      var seen  = 0
      var bytes = 0
      val t0    = System.currentTimeMillis

      while (i <= completeChunks) {
        val (AsyncParse(errors, results), parser) = if (i <= completeChunks) {
          val data: Array[Byte] = utf8Bytes(i match {
            case 0                       => firstChunk
            case _ if i < completeChunks => chunk
            case _                       => lastChunk
          })
          bytes += data.length
          p(data)
        }
        else {
          p.finish()
          p.apply("")
        }
        if (!errors.isEmpty) throw errors.head
        seen += results.length
        p = parser
        i += 1
      }

      val ms = System.currentTimeMillis() - t0
      (seen, bytes, ms)
    }

    def verifyAndTime(tpl: (Int, Int, Long), expected: Int): (Int, Long) = {
      tpl._1 must_== expected
      (tpl._2, tpl._3)
    }

    val (b1, t1) = verifyAndTime(sync(elem, num), 1)
    val (b2, t2) = verifyAndTime(syncStream(elem, num), num)
    val (b3, t3) = verifyAndTime(async(AsyncParser.stream(), true, elem, num), 1)
    val (b4, t4) = verifyAndTime(async(AsyncParser.stream(), false, elem, num), num)
    val (b5, t5) = verifyAndTime(async(AsyncParser.unwrap(), true, elem, num), num)

    b2 must_== b4
    b1 must_== b3
    b1 must_== b5
    println("parsed array (%d bytes):  sync=%dms  async=%dms  unpacked=%dms" format (b1, t1, t3, t5))
    println("parsed stream (%d bytes): sync=%dms  async=%dms" format (b2, t2, t4))
    ok
  }
}
