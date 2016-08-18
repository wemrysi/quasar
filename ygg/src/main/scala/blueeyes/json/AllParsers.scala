package blueeyes
package json

import scala.math.max
import java.lang.Character.isHighSurrogate
import scalaz._
import Validation.fromTryCatchNonFatal
import java.lang.Integer.parseInt

case class AsyncParse(errors: Seq[ParseException], values: Seq[JValue])

/**
  * This class is used internally by AsyncParser to signal that we've reached
  * the end of the particular input we were given.
  */
private class AsyncException extends Exception

private class FailureException extends Exception

object AsyncParser {
  final val MB1 = 1048576

  sealed trait Input
  case class More(buf: ByteBuffer) extends Input
  case object Done                 extends Input

  /**
    * Asynchronous parser for a stream of (whitespace-delimited) JSON values.
    */
  def stream(): AsyncParser =
    new AsyncParser(state = -1, curr = 0, stack = Nil, data = new Array[Byte](131072), len = 0, allocated = 131072, offset = 0, done = false, streamMode = 0)

  /**
    * Asynchronous parser for a single JSON value.
    */
  def json(): AsyncParser =
    new AsyncParser(state = -1, curr = 0, stack = Nil, data = new Array[Byte](131072), len = 0, allocated = 131072, offset = 0, done = false, streamMode = -1)

  /**
    * Asynchronous parser which can unwrap a single JSON array into a stream of
    * values (or return a single value otherwise).
    */
  def unwrap(): AsyncParser =
    new AsyncParser(state = -5, curr = 0, stack = Nil, data = new Array[Byte](131072), len = 0, allocated = 131072, offset = 0, done = false, streamMode = 1)
}

/*
 * AsyncParser is able to parse chunks of data (encoded as
 * Option[ByteBuffer] instances) and parse asynchronously.  You can
 * use the factory methods in the companion object to instantiate an
 * async parser.
 *
 * The async parser's fields are described below:
 *
 * The (state, curr, stack) triple is used to save and restore parser
 * state between async calls. State also helps encode extra
 * information when streaming or unwrapping an array.
 *
 * The (data, len, allocated) triple is used to manage the underlying
 * data the parser is keeping track of. As new data comes in, data may
 * be expanded if not enough space is available.
 *
 * The offset parameter is used to drive the outer async parsing. It
 * stores similar information to curr but is kept separate to avoid
 * "corrupting" our snapshot.
 *
 * The done parameter is used internally to help figure out when the
 * atEof() parser method should return true. This will be set when
 * apply(None) is called.
 *
 * The streamMode parameter controls how the asynchronous parser will
 * be handling multiple values. There are three states:
 *
 *    1: An array is being unwrapped. Normal JSON array rules apply
 *       (Note that if the outer value observed is not an array, this
 *       mode will toggle to the -1 mode).
 *
 *    0: A JSON stream is being parsed. JSON values will be separated
 *       by optional whitespace
 *
 *   -1: No streaming is occuring. Only a single JSON value is
 *       allowed.
 */
final class AsyncParser protected[json] (
    protected[json] var state: Int,
    protected[json] var curr: Int,
    protected[json] var stack: List[Context],
    protected[json] var data: Array[Byte],
    protected[json] var len: Int,
    protected[json] var allocated: Int,
    protected[json] var offset: Int,
    protected[json] var done: Boolean,
    protected[json] var streamMode: Int
) extends ByteBasedParser {
  import AsyncParser._

  protected[this] var line = 0
  protected[this] var pos  = 0
  protected[this] final def newline(i: Int) { line += 1; pos = i + 1 }
  protected[this] final def column(i: Int) = i - pos

  protected[this] final def copy() =
    new AsyncParser(state, curr, stack, data.clone, len, allocated, offset, done, streamMode)

  final def apply(input: Input): (AsyncParse, AsyncParser) = copy.feed(input)

  protected[this] final def absorb(buf: ByteBuffer): Unit = {
    done = false
    val buflen = buf.limit - buf.position
    val need   = len + buflen

    // if we don't have enough free space available we'll need to grow our
    // data array. we never shrink the data array, assuming users will call
    // feed with similarly-sized buffers.
    if (need > allocated) {
      val doubled = if (allocated < 0x40000000) allocated * 2 else Int.MaxValue
      val newsize = max(need, doubled)
      val newdata = new Array[Byte](newsize)
      System.arraycopy(data, 0, newdata, 0, len)
      data = newdata
      allocated = newsize
    }

    buf.get(data, len, buflen)
    len = need
  }

  // Explanation of the new synthetic states .The parser machinery
  // uses positive integers for states while parsing json values. We
  // use these negative states to keep track of the async parser's
  // status between json values.
  //
  // ASYNC_PRESTART: We haven't seen any non-whitespace yet. We
  // could be parsing an array, or not. We are waiting for valid
  // JSON.
  //
  // ASYNC_START: We've seen an array and have begun unwrapping
  // it. We could see a ] if the array is empty, or valid JSON.
  //
  // ASYNC_END: We've parsed an array and seen the final ]. At this
  // point we should only see whitespace or an EOF.
  //
  // ASYNC_POSTVAL: We just parsed a value from inside the array. We
  // expect to see whitespace, a comma, or an EOF.
  //
  // ASYNC_PREVAL: We are in an array and we just saw a comma. We
  // expect to see whitespace or a JSON value.
  @inline private[this] final def ASYNC_PRESTART = -5
  @inline private[this] final def ASYNC_START    = -4
  @inline private[this] final def ASYNC_END      = -3
  @inline private[this] final def ASYNC_POSTVAL  = -2
  @inline private[this] final def ASYNC_PREVAL   = -1

  protected[json] def feed(b: Input): (AsyncParse, AsyncParser) = {
    b match {
      case Done      => done = true
      case More(buf) => absorb(buf)
    }

    // accumulates errors and results
    val errors  = ArrayBuffer.empty[ParseException]
    val results = ArrayBuffer.empty[JValue]

    // we rely on exceptions to tell us when we run out of data
    try {
      while (true) {
        if (state < 0) {
          (at(offset): @switch) match {
            case '\n' =>
              newline(offset)
              offset += 1

            case ' ' | '\t' | '\r' =>
              offset += 1

            case '[' =>
              if (state == ASYNC_PRESTART) {
                offset += 1
                state = ASYNC_START
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else if (state == ASYNC_POSTVAL) {
                die(offset, "expected , or ]")
              } else {
                state = 0
              }

            case ',' =>
              if (state == ASYNC_POSTVAL) {
                offset += 1
                state = ASYNC_PREVAL
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else {
                die(offset, "expected json value")
              }

            case ']' =>
              if (state == ASYNC_POSTVAL || state == ASYNC_START) {
                if (streamMode > 0) {
                  offset += 1
                  state = ASYNC_END
                } else {
                  die(offset, "expected json value or eof")
                }
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else {
                die(offset, "expected json value")
              }

            case c =>
              if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else if (state == ASYNC_POSTVAL) {
                die(offset, "expected ] or ,")
              } else {
                if (state == ASYNC_PRESTART && streamMode > 0) streamMode = -1
                state = 0
              }
          }

        } else {
          // jump straight back into rparse
          offset = reset(offset)
          val (value, j) = if (state <= 0) {
            parse(offset)
          } else {
            rparse(state, curr, stack)
          }
          if (streamMode > 0) {
            state = ASYNC_POSTVAL
          } else if (streamMode == 0) {
            state = ASYNC_PREVAL
          } else {
            state = ASYNC_END
          }
          curr = j
          offset = j
          stack = Nil
          results.append(value)
        }
      }
    } catch {
      case e: AsyncException =>
      // we ran out of data, so return what we have so far

      case e: ParseException =>
        // we hit a parser error, so return that error and results so far
        errors.append(e)
    }
    (AsyncParse(errors, results), this)
  }

  // every 1M we shift our array back by 1M.
  protected[this] final def reset(i: Int): Int = (
    if (offset >= MB1) {
      len -= MB1
      offset -= MB1
      pos -= MB1
      System.arraycopy(data, MB1, data, 0, len)
      i - MB1
    }
    else i
  )

  /**
    * We use this to keep track of the last recoverable place we've
    * seen. If we hit an AsyncException, we can later resume from this
    * point.
    *
    * This method is called during every loop of rparse, and the
    * arguments are the exact arguments we can pass to rparse to
    * continue where we left off.
    */
  protected[this] final def checkpoint(state: Int, i: Int, stack: List[Context]) {
    this.state = state
    this.curr = i
    this.stack = stack
  }

  /**
    * This is a specialized accessor for the case where our underlying data are
    * bytes not chars.
    */
  protected[this] final def byte(i: Int): Byte =
    if (i >= len)
      throw new AsyncException
    else
      data(i)

  // we need to signal if we got out-of-bounds
  protected[this] final def at(i: Int): Char =
    if (i >= len)
      throw new AsyncException
    else
      data(i).toChar

  /**
    * Access a byte range as a string.
    *
    * Since the underlying data are UTF-8 encoded, i and k must occur on unicode
    * boundaries. Also, the resulting String is not guaranteed to have length
    * (k - i).
    */
  protected[this] final def at(i: Int, k: Int): String = {
    if (k > len) throw new AsyncException
    val size = k - i
    val arr  = new Array[Byte](size)
    System.arraycopy(data, i, arr, 0, size)
    new String(arr, Utf8Charset)
  }

  // the basic idea is that we don't signal EOF until done is true, which means
  // the client explicitly send us an EOF.
  protected[this] final def atEof(i: Int) = done && i >= len

  // we don't have to do anything special on close.
  protected[this] final def close() = ()
}

/**
  * Trait used when the data to be parsed is in UTF-8.
  */
private[json] trait ByteBasedParser extends Parser {
  protected[this] def byte(i: Int): Byte

  /**
    * See if the string has any escape sequences. If not, return the end of the
    * string. If so, bail out and return -1.
    *
    * This method expects the data to be in UTF-8 and accesses it as bytes. Thus
    * we can just ignore any bytes with the highest bit set.
    */
  protected[this] final def parseStringSimple(i: Int, ctxt: Context): Int = {
    var j = i
    var c = byte(j)
    while (c != 34) {
      if (c == 92) return -1
      j += 1
      c = byte(j)
    }
    j + 1
  }

  /**
    * Parse the string according to JSON rules, and add to the given context.
    *
    * This method expects the data to be in UTF-8 and accesses it as bytes.
    */
  protected[this] final def parseString(i: Int, ctxt: Context): Int = {
    val k = parseStringSimple(i + 1, ctxt)
    if (k != -1) {
      ctxt.add(at(i + 1, k - 1))
      return k
    }

    var j  = i + 1
    val sb = new CharBuilder

    var c = byte(j)
    while (c != 34) { // "
      if (c == 92) { // \
        (byte(j + 1): @switch) match {
          case 98  => { sb.append('\b'); j += 2 }
          case 102 => { sb.append('\f'); j += 2 }
          case 110 => { sb.append('\n'); j += 2 }
          case 114 => { sb.append('\r'); j += 2 }
          case 116 => { sb.append('\t'); j += 2 }

          // if there's a problem then descape will explode
          case 117 => { sb.append(descape(at(j + 2, j + 6))); j += 6 }

          // permissive: let any escaped char through, not just ", / and \
          case c2 => { sb.append(c2.toChar); j += 2 }
        }
      } else if (c < 128) {
        // 1-byte UTF-8 sequence
        sb.append(c.toChar)
        j += 1
      } else if ((c & 224) == 192) {
        // 2-byte UTF-8 sequence
        sb.extend(at(j, j + 2))
        j += 2
      } else if ((c & 240) == 224) {
        // 3-byte UTF-8 sequence
        sb.extend(at(j, j + 3))
        j += 3
      } else if ((c & 248) == 240) {
        // 4-byte UTF-8 sequence
        sb.extend(at(j, j + 4))
        j += 4
      } else {
        die(j, "invalid UTF-8 encoding")
      }
      c = byte(j)
    }
    ctxt.add(sb.makeString)
    j + 1
  }
}

/**
  * Basic ByteBuffer parser.
  */
private final class ByteBufferParser(src: ByteBuffer) extends SyncParser with ByteBasedParser {
  final val start = src.position
  final val limit = src.limit - start

  var line = 0
  protected[this] final def newline(i: Int) { line += 1 }
  protected[this] final def column(i: Int) = i

  final def close() { src.position(src.limit) }
  final def reset(i: Int): Int = i
  final def checkpoint(state: Int, i: Int, stack: List[Context]) {}
  final def byte(i: Int): Byte = src.get(i + start)
  final def at(i: Int): Char   = src.get(i + start).toChar

  final def at(i: Int, k: Int): String = {
    val len = k - i
    val arr = new Array[Byte](len)
    src.position(i + start)
    src.get(arr, 0, len)
    src.position(start)
    new String(arr, Utf8Charset)
  }

  final def atEof(i: Int) = i >= limit
}

/**
  * Trait used when the data to be parsed is in UTF-16.
  */
private trait CharBasedParser extends Parser {

  /**
    * See if the string has any escape sequences. If not, return the end of the
    * string. If so, bail out and return -1.
    *
    * This method expects the data to be in UTF-16 and accesses it as chars.
    * In a few cases we might bail out incorrectly (by reading the second-half
    * of a surrogate pair as \\) but for now the belief is that checking every
    * character would be more expensive. So... in those cases we'll fall back to
    * the slower (correct) UTF-16 parsing.
    */
  protected[this] final def parseStringSimple(i: Int, ctxt: Context): Int = {
    var j = i
    var c = at(j)
    while (c != '"') {
      if (c < ' ') die(j, "control char in string")
      if (c == '\\') return -1
      j += 1
      c = at(j)
    }
    j + 1
  }

  /**
    * Parse the string according to JSON rules, and add to the given context.
    *
    * This method expects the data to be in UTF-16, and access it as Char. It
    * performs the correct checks to make sure that we don't interpret a
    * multi-char code point incorrectly.
    */
  protected[this] final def parseString(i: Int, ctxt: Context): Int = {
    val k = parseStringSimple(i + 1, ctxt)
    if (k != -1) {
      ctxt.add(at(i + 1, k - 1))
      return k
    }

    var j  = i + 1
    val sb = new CharBuilder

    var c = at(j)
    while (c != '"') {
      if (c < ' ') {
        die(j, "control char in string")
      } else if (c == '\\') {
        (at(j + 1): @switch) match {
          case 'b' => { sb.append('\b'); j += 2 }
          case 'f' => { sb.append('\f'); j += 2 }
          case 'n' => { sb.append('\n'); j += 2 }
          case 'r' => { sb.append('\r'); j += 2 }
          case 't' => { sb.append('\t'); j += 2 }

          // if there's a problem then descape will explode
          case 'u' => { sb.append(descape(at(j + 2, j + 6))); j += 6 }

          // permissive: let any escaped char through, not just ", / and \
          case c2 => { sb.append(c2); j += 2 }
        }
      } else if (isHighSurrogate(c)) {
        // this case dodges the situation where we might incorrectly parse the
        // second Char of a unicode code point.
        sb.append(c)
        sb.append(at(j + 1))
        j += 2
      } else {
        // this case is for "normal" code points that are just one Char.
        sb.append(c)
        j += 1
      }
      j = reset(j)
      c = at(j)
    }
    ctxt.add(sb.makeString)
    j + 1
  }
}

object JParser {
  type Result[A] = Validation[Throwable, A]

  def parseUnsafe(str: String): JValue = new StringParser(str).parse()

  def parseFromString(str: String): Result[JValue]          = fromTryCatchNonFatal(new StringParser(str).parse())
  def parseManyFromString(str: String): Result[Seq[JValue]] = fromTryCatchNonFatal(new StringParser(str).parseMany())

  def parseFromByteBuffer(buf: ByteBuffer): Result[JValue]          = fromTryCatchNonFatal(new ByteBufferParser(buf).parse())
  def parseManyFromByteBuffer(buf: ByteBuffer): Result[Seq[JValue]] = fromTryCatchNonFatal(new ByteBufferParser(buf).parseMany())
}

case class ParseException(msg: String, index: Int, line: Int, col: Int) extends Exception(msg)
case class IncompleteParseException(msg: String)                        extends Exception(msg)

/**
  * Parser contains the state machine that does all the work. The only
  */
private trait Parser {

  /**
    * Read the byte/char at 'i' as a Char.
    *
    * Note that this should not be used on potential multi-byte sequences.
    */
  protected[this] def at(i: Int): Char

  /**
    * Read the bytes/chars from 'i' until 'j' as a String.
    */
  protected[this] def at(i: Int, j: Int): String

  /**
    * Return true iff 'i' is at or beyond the end of the input (EOF).
    */
  protected[this] def atEof(i: Int): Boolean

  /**
    * Return true iff the byte/char at 'i' is equal to 'c'.
    */
  protected[this] final def is(i: Int, c: Char): Boolean = at(i) == c

  /**
    * Return true iff the bytes/chars from 'i' until 'j' are equal to 'str'.
    */
  protected[this] final def is(i: Int, j: Int, str: String): Boolean = at(i, j) == str

  /**
    * The reset() method is used to signal that we're working from the given
    * position, and any previous data can be released. Some parsers (e.g.
    * StringParser) will ignore release, while others (e.g. PathParser) will
    * need to use this information to release and allocate different areas.
    */
  protected[this] def reset(i: Int): Int

  /**
    * The checkpoint() method is used to allow some parsers to store their
    * progress.
    */
  protected[this] def checkpoint(state: Int, i: Int, stack: List[Context]): Unit

  /**
    * Should be called when parsing is finished.
    */
  protected[this] def close(): Unit

  /**
    * Valid parser states.
    */
  @inline protected[this] final val ARRBEG = 6
  @inline protected[this] final val OBJBEG = 7
  @inline protected[this] final val DATA   = 1
  @inline protected[this] final val KEY    = 2
  @inline protected[this] final val SEP    = 3
  @inline protected[this] final val ARREND = 4
  @inline protected[this] final val OBJEND = 5

  protected[this] def newline(i: Int): Unit
  protected[this] def line(): Int
  protected[this] def column(i: Int): Int

  /**
    * Used to generate error messages with character info and byte addresses.
    */
  protected[this] def die(i: Int, msg: String) = {
    val y = line() + 1
    val x = column(i) + 1
    val s = "%s got %s (line %d, column %d)" format (msg, at(i), y, x)
    throw ParseException(s, i, y, x)
  }

  /**
    * Used to generate messages for internal errors.
    */
  protected[this] def error(msg: String) =
    sys.error(msg)

  /**
    * Parse the given number, and add it to the given context.
    *
    * We don't actually instantiate a number here, but rather save the string
    * for future use. This ends up being way faster and has the nice side-effect
    * that we know exactly how the user represented the number.
    *
    * It would probably be possible to keep track of the whether the number is
    * expected to be whole, decimal, etc. but we don't do that at the moment.
    */
  protected[this] final def parseNum(i: Int, ctxt: Context): Int = {
    var j = i
    var c = at(j)

    if (c == '-') {
      j += 1
      c = at(j)
    }
    while ('0' <= c && c <= '9') { j += 1; c = at(j) }

    if (c == '.') {
      j += 1
      c = at(j)
      while ('0' <= c && c <= '9') { j += 1; c = at(j) }
    }

    if (c == 'e' || c == 'E') {
      j += 1
      c = at(j)
      if (c == '+' || c == '-') {
        j += 1
        c = at(j)
      }
      while ('0' <= c && c <= '9') { j += 1; c = at(j) }
    }

    ctxt.add(JNum(at(i, j)))
    j
  }

  /**
    * This number parser is a bit slower because it has to be sure it doesn't
    * run off the end of the input. Normally (when operating in rparse in the
    * context of an outer array or objedct) we don't have to worry about this
    * and can just grab characters, because if we run out of characters that
    * would indicate bad input.
    *
    * This method has all the same caveats as the previous method.
    */
  protected[this] final def parseNumSlow(i: Int, ctxt: Context): Int = {
    var j = i
    var c = at(j)

    if (c == '-') {
      // any valid input will require at least one digit after -
      j += 1
      c = at(j)
    }
    while ('0' <= c && c <= '9') {
      j += 1
      if (atEof(j)) {
        ctxt.add(JNum(at(i, j)))
        return j
      }
      c = at(j)
    }

    if (c == '.') {
      // any valid input will require at least one digit after .
      j += 1
      c = at(j)
      while ('0' <= c && c <= '9') {
        j += 1
        if (atEof(j)) {
          ctxt.add(JNum(at(i, j)))
          return j
        }
        c = at(j)
      }
    }

    if (c == 'e' || c == 'E') {
      // any valid input will require at least one digit after e, e+, etc
      j += 1
      c = at(j)
      if (c == '+' || c == '-') {
        j += 1
        c = at(j)
      }
      while ('0' <= c && c <= '9') {
        j += 1
        if (atEof(j)) {
          ctxt.add(JNum(at(i, j)))
          return j
        }
        c = at(j)
      }
    }
    ctxt.add(JNum(at(i, j)))
    j
  }

  /**
    * Generate a Char from the hex digits of "\u1234" (i.e. "1234").
    *
    * NOTE: This is only capable of generating characters from the basic plane.
    * This is why it can only return Char instead of Int.
    */
  protected[this] final def descape(s: String) = parseInt(s, 16).toChar

  /**
    * Parse the JSON string starting at 'i' and save it into 'ctxt'.
    */
  protected[this] def parseString(i: Int, ctxt: Context): Int

  /**
    * Parse the JSON constant "true".
    */
  protected[this] final def parseTrue(i: Int) =
    if (is(i, i + 4, "true")) JTrue else die(i, "expected true")

  /**
    * Parse the JSON constant "false".
    */
  protected[this] final def parseFalse(i: Int) =
    if (is(i, i + 5, "false")) JFalse else die(i, "expected false")

  /**
    * Parse the JSON constant "null".
    */
  protected[this] final def parseNull(i: Int) =
    if (is(i, i + 4, "null")) JNull else die(i, "expected null")

  /**
    * Parse and return the "next" JSON value as well as the position beyond it.
    * This method is used by both parse() as well as parseMany().
    */
  protected[this] final def parse(i: Int): (JValue, Int) =
    try {
      (at(i): @switch) match {
        // ignore whitespace
        case ' '  => parse(i + 1)
        case '\t' => parse(i + 1)
        case '\r' => parse(i + 1)
        case '\n' => newline(i); parse(i + 1)

        // if we have a recursive top-level structure, we'll delegate the parsing
        // duties to our good friend rparse().
        case '[' => rparse(ARRBEG, i + 1, new ArrContext :: Nil)
        case '{' => rparse(OBJBEG, i + 1, new ObjContext :: Nil)

        // we have a single top-level number
        case '-' | '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
          val ctxt = new SingleContext
          val j    = parseNumSlow(i, ctxt)
          (ctxt.value, j)

        // we have a single top-level string
        case '"' =>
          val ctxt = new SingleContext
          val j    = parseString(i, ctxt)
          (ctxt.value, j)

        // we have a single top-level constant
        case 't' => (parseTrue(i), i + 4)
        case 'f' => (parseFalse(i), i + 5)
        case 'n' => (parseNull(i), i + 4)

        // invalid
        case _ => die(i, "expected json value")
      }
    } catch {
      case _: IndexOutOfBoundsException =>
        throw IncompleteParseException("exhausted input")
    }

  /**
    * Tail-recursive parsing method to do the bulk of JSON parsing.
    *
    * This single method manages parser states, data, etc. Except for parsing
    * non-recursive values (like strings, numbers, and constants) all important
    * work happens in this loop (or in methods it calls, like reset()).
    *
    * Currently the code is optimized to make use of switch statements. Future
    * work should consider whether this is better or worse than manually
    * constructed if/else statements or something else.
    */
  @tailrec
  protected[this] final def rparse(state: Int, j: Int, stack: List[Context]): (JValue, Int) = {
    val i = reset(j)
    checkpoint(state, i, stack)
    (state: @switch) match {
      // we are inside an object or array expecting to see data
      case DATA =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case '[' => rparse(ARRBEG, i + 1, new ArrContext :: stack)
          case '{' => rparse(OBJBEG, i + 1, new ObjContext :: stack)

          case '-' | '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
            val ctxt = stack.head
            val j    = parseNum(i, ctxt)
            rparse(if (ctxt.isObj) OBJEND else ARREND, j, stack)

          case '"' =>
            val ctxt = stack.head
            val j    = parseString(i, ctxt)
            rparse(if (ctxt.isObj) OBJEND else ARREND, j, stack)

          case 't' =>
            val ctxt = stack.head
            ctxt.add(parseTrue(i))
            rparse(if (ctxt.isObj) OBJEND else ARREND, i + 4, stack)

          case 'f' =>
            val ctxt = stack.head
            ctxt.add(parseFalse(i))
            rparse(if (ctxt.isObj) OBJEND else ARREND, i + 5, stack)

          case 'n' =>
            val ctxt = stack.head
            ctxt.add(parseNull(i))
            rparse(if (ctxt.isObj) OBJEND else ARREND, i + 4, stack)

          case _ =>
            die(i, "expected json value")
        }

      // we are in an object expecting to see a key
      case KEY =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case '"' =>
            val j = parseString(i, stack.head)
            rparse(SEP, j, stack)

          case _ => die(i, "expected \"")
        }

      // we are starting an array, expecting to see data or a closing bracket
      case ARRBEG =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case ']' =>
            stack match {
              case ctxt1 :: Nil =>
                (ctxt1.finish, i + 1)
              case ctxt1 :: ctxt2 :: tail =>
                ctxt2.add(ctxt1.finish)
                rparse(if (ctxt2.isObj) OBJEND else ARREND, i + 1, ctxt2 :: tail)
              case _ =>
                error("invalid stack")
            }

          case _ => rparse(DATA, i, stack)
        }

      // we are starting an object, expecting to see a key or a closing brace
      case OBJBEG =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case '}' =>
            stack match {
              case ctxt1 :: Nil =>
                (ctxt1.finish, i + 1)
              case ctxt1 :: ctxt2 :: tail =>
                ctxt2.add(ctxt1.finish)
                rparse(if (ctxt2.isObj) OBJEND else ARREND, i + 1, ctxt2 :: tail)
              case _ =>
                error("invalid stack")
            }

          case _ => rparse(KEY, i, stack)
        }

      // we are in an object just after a key, expecting to see a colon
      case SEP =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case ':' => rparse(DATA, i + 1, stack)

          case _ => die(i, "expected :")
        }

      // we are at a possible stopping point for an array, expecting to see
      // either a comma (before more data) or a closing bracket.
      case ARREND =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case ',' => rparse(DATA, i + 1, stack)

          case ']' =>
            stack match {
              case ctxt1 :: Nil =>
                (ctxt1.finish, i + 1)
              case ctxt1 :: ctxt2 :: tail =>
                ctxt2.add(ctxt1.finish)
                rparse(if (ctxt2.isObj) OBJEND else ARREND, i + 1, ctxt2 :: tail)
              case _ =>
                error("invalid stack")
            }

          case _ => die(i, "expected ] or ,")
        }

      // we are at a possible stopping point for an object, expecting to see
      // either a comma (before more data) or a closing brace.
      case OBJEND =>
        (at(i): @switch) match {
          case ' '  => rparse(state, i + 1, stack)
          case '\t' => rparse(state, i + 1, stack)
          case '\r' => rparse(state, i + 1, stack)
          case '\n' => newline(i); rparse(state, i + 1, stack)

          case ',' => rparse(KEY, i + 1, stack)

          case '}' =>
            stack match {
              case ctxt1 :: Nil =>
                (ctxt1.finish, i + 1)
              case ctxt1 :: ctxt2 :: tail =>
                ctxt2.add(ctxt1.finish)
                rparse(if (ctxt2.isObj) OBJEND else ARREND, i + 1, ctxt2 :: tail)
              case _ =>
                error("invalid stack")
            }

          case _ => die(i, "expected } or ,")
        }
    }
  }
}

/**
  * Basic in-memory string parsing.
  *
  * This parser is limited to the maximum string size (~2G). Obviously for large
  * JSON documents it's better to avoid using this parser and go straight from
  * disk, to avoid having to load the whole thing into memory at once.
  */
private final class StringParser(s: String) extends SyncParser with CharBasedParser {
  var line                                                             = 0
  final def column(i: Int): Int                                        = i
  final def newline(i: Int): Unit                                      = line += 1
  final def reset(i: Int): Int                                         = i
  final def checkpoint(state: Int, i: Int, stack: List[Context]): Unit = ()
  final def at(i: Int): Char                                           = s.charAt(i)
  final def at(i: Int, j: Int): String                                 = s.substring(i, j)
  final def atEof(i: Int): Boolean                                     = i == s.length
  final def close(): Unit                                              = ()
}

private trait SyncParser extends Parser {

  /**
    * Parse the JSON document into a single JSON value.
    *
    * The parser considers documents like '333', 'true', and '"foo"' to be
    * valid, as well as more traditional documents like [1,2,3,4,5]. However,
    * multiple top-level objects are not allowed.
    */
  final def parse(): JValue = {
    val (value, i) = parse(0)
    var j          = i
    while (!atEof(j)) {
      (at(j): @switch) match {
        case '\n'              => newline(j); j += 1
        case ' ' | '\t' | '\r' => j += 1
        case _                 => die(j, "expected whitespace or eof")
      }
    }
    if (!atEof(j)) die(j, "expected eof")
    close()
    value
  }

  /**
    * Parse the given document into a sequence of JSON values. These might be
    * containers like objects and arrays, or primtitives like numbers and
    * strings.
    *
    * JSON objects may only be separated by whitespace. Thus, "top-level" commas
    * and other characters will become parse errors.
    */
  final def parseMany(): Seq[JValue] = {
    val results = ArrayBuffer.empty[JValue]
    var i       = 0
    while (!atEof(i)) {
      (at(i): @switch) match {
        case '\n'              => newline(i); i += 1
        case ' ' | '\t' | '\r' => i += 1
        case _ =>
          val (value, j) = parse(i)
          results.append(value)
          i = j
      }
    }
    close()
    results
  }
}
