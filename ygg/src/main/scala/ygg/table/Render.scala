package ygg.table

import scalaz._, Scalaz._
import ygg._, common._, data._

object Render {
  def renderTable(table: Table, prefix: String, delimiter: String, suffix: String): StreamT[Need, CharBuffer] = {
    val slices = table.slices
    def wrap(stream: StreamT[M, CharBuffer]) = {
      if (prefix == "" && suffix == "") stream
      else if (suffix == "") charBuffer(prefix) :: stream
      else if (prefix == "") stream ++ singleStreamT(charBuffer(suffix))
      else charBuffer(prefix) :: (stream ++ singleStreamT(charBuffer(suffix)))
    }

    def foldFlatMap(slices: StreamT[M, Slice], rendered: Boolean): StreamT[M, CharBuffer] = {
      StreamT[M, CharBuffer](slices.step map {
        case StreamT.Skip(tail)         => StreamT.Skip(foldFlatMap(tail(), rendered))
        case StreamT.Done               => StreamT.Done
        case StreamT.Yield(slice, tail) =>
          val (stream, rendered2) = Render.renderSlice(slice, delimiter)
          val stream2             = if (rendered && rendered2) charBuffer(delimiter) :: stream else stream

          StreamT.Skip(stream2 ++ foldFlatMap(tail(), rendered || rendered2))
      })
    }

    wrap(foldFlatMap(slices, false))
  }

  def renderSlice(self: Slice, delimiter: String): (StreamT[M, CharBuffer], Boolean) = {
    import self._

    if (columns.isEmpty) {
      (emptyStreamT[CharBuffer], false)
    } else {
      val BufferSize = 1024 * 10 // 10 KB

      val optSchema = {
        def insert(target: SchemaNode, ref: ColumnRef, col: Column): SchemaNode = {
          val ColumnRef(selector, ctype) = ref

          selector.nodes match {
            case CPathField(name) :: tail => {
              target match {
                case SchemaNode.Obj(nodes) => {
                  val subTarget = nodes get name getOrElse SchemaNode.Union(Set())
                  val result    = insert(subTarget, ColumnRef(CPath(tail), ctype), col)
                  SchemaNode.Obj(nodes + (name -> result))
                }

                case SchemaNode.Union(nodes) => {
                  val objNode = nodes find {
                    case _: SchemaNode.Obj => true
                    case _                 => false
                  }

                  val subTarget = objNode getOrElse SchemaNode.Obj(Map())
                  SchemaNode.Union(nodes - subTarget + insert(subTarget, ref, col))
                }

                case node =>
                  SchemaNode.Union(Set(node, insert(SchemaNode.Obj(Map()), ref, col)))
              }
            }

            case CPathIndex(idx) :: tail => {
              target match {
                case SchemaNode.Arr(map) => {
                  val subTarget = map get idx getOrElse SchemaNode.Union(Set())
                  val result    = insert(subTarget, ColumnRef(CPath(tail), ctype), col)
                  SchemaNode.Arr(map + (idx -> result))
                }

                case SchemaNode.Union(nodes) => {
                  val objNode = nodes find {
                    case _: SchemaNode.Arr => true
                    case _                 => false
                  }

                  val subTarget = objNode getOrElse SchemaNode.Arr(Map())
                  SchemaNode.Union(nodes - subTarget + insert(subTarget, ref, col))
                }

                case node =>
                  SchemaNode.Union(Set(node, insert(SchemaNode.Arr(Map()), ref, col)))
              }
            }

            case CPathMeta(_) :: _ => target

            case CPathArray :: _ => ???

            case Nil => {
              val node = SchemaNode.Leaf(ctype, col)

              target match {
                case SchemaNode.Union(nodes) => SchemaNode.Union(nodes + node)
                case oldNode                 => SchemaNode.Union(Set(oldNode, node))
              }
            }
          }
        }

        def normalize(schema: SchemaNode): Option[SchemaNode] = schema match {
          case SchemaNode.Obj(nodes) => {
            val nodes2 = nodes flatMap {
              case (key, value) => normalize(value) map { key -> _ }
            }

            val back =
              if (nodes2.isEmpty)
                None
              else
                Some(SchemaNode.Obj(nodes2))

            back foreach { obj =>
              obj.keys = new Array[String](nodes2.size)
              obj.values = new Array[SchemaNode](nodes2.size)
            }

            var i = 0
            back foreach { obj =>
              for ((key, value) <- nodes2) {
                obj.keys(i) = key
                obj.values(i) = value
                i += 1
              }
            }

            back
          }

          case SchemaNode.Arr(map) => {
            val map2 = map flatMap {
              case (idx, value) => normalize(value) map { idx -> _ }
            }

            val back =
              if (map2.isEmpty)
                None
              else
                Some(SchemaNode.Arr(map2))

            back foreach { arr =>
              arr.nodes = new Array[SchemaNode](map2.size)
            }

            var i = 0
            back foreach { arr =>
              val values = map2.toSeq sortBy { _._1 } map { _._2 }

              for (value <- values) {
                arr.nodes(i) = value
                i += 1
              }
            }

            back
          }

          case SchemaNode.Union(nodes) => {
            val nodes2 = nodes flatMap normalize

            if (nodes2.isEmpty)
              None
            else if (nodes2.size == 1)
              nodes2.headOption
            else {
              val union = SchemaNode.Union(nodes2)
              union.possibilities = nodes2.toArray
              Some(union)
            }
          }

          case lf: SchemaNode.Leaf => Some(lf)
        }

        val schema = columns.foldLeft(SchemaNode.Union(Set()): SchemaNode) {
          case (acc, (ref, col)) => insert(acc, ref, col)
        }

        normalize(schema)
      }

      // don't remove!  @tailrec bugs if you use optSchema.map
      if (optSchema.isDefined) {
        val schema = optSchema.get

        val depth = {
          def loop(schema: SchemaNode): Int = schema match {
            case obj: SchemaNode.Obj =>
              4 + (obj.values map loop max)

            case arr: SchemaNode.Arr =>
              2 + (arr.nodes map loop max)

            case union: SchemaNode.Union =>
              union.possibilities map loop max

            case SchemaNode.Leaf(_, _) => 0
          }

          loop(schema)
        }

        // we have the schema, now emit

        var buffer = charBuffer(BufferSize)
        val vector = new ArrayBuffer[CharBuffer](math.max(1, size / 10))

        @inline
        def checkPush(length: Int) {
          if (buffer.remaining < length) {
            buffer.flip()
            vector += buffer

            buffer = charBuffer(BufferSize)
          }
        }

        @inline
        def push(c: Char) {
          checkPush(1)
          buffer.put(c)
        }

        @inline
        def pushStr(str: String) {
          checkPush(str.length)
          buffer.put(str)
        }

        val in      = new RingDeque[String](depth + 1)
        val inFlags = new RingDeque[Boolean](depth + 1)

        @inline
        def pushIn(str: String, flag: Boolean) {
          in.pushBack(str)
          inFlags.pushBack(flag)
        }

        @inline
        def popIn() {
          in.popBack()
          inFlags.popBack()
        }

        @inline
        @tailrec
        def flushIn() {
          if (!in.isEmpty) {
            val str = in.popFront()

            val flag = inFlags.popFront()

            if (flag) {
              renderString(str)
            } else {
              checkPush(str.length)
              buffer.put(str)
            }

            flushIn()
          }
        }

        // emitters

        @inline
        @tailrec
        def renderString(str: String, idx: Int = 0) {
          if (idx == 0) {
            push('"')
          }

          if (idx < str.length) {
            val c = str.charAt(idx)

            (c: @switch) match {
              case '"'  => pushStr("\\\"")
              case '\\' => pushStr("\\\\")
              case '\b' => pushStr("\\b")
              case '\f' => pushStr("\\f")
              case '\n' => pushStr("\\n")
              case '\r' => pushStr("\\r")
              case '\t' => pushStr("\\t")

              case c => {
                if ((c >= '\u0000' && c < '\u001f') || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) {
                  pushStr("\\u")
                  pushStr("%04x".format(Character.codePointAt(str, idx)))
                } else {
                  push(c)
                }
              }
            }

            renderString(str, idx + 1)
          } else {
            push('"')
          }
        }

        @inline
        def renderLong(ln: Long) {

          @inline
          @tailrec
          def power10(ln: Long, seed: Long = 1): Long = {
            // note: we could be doing binary search here

            if (seed * 10 < 0) // overflow
              seed
            else if (seed * 10 > ln)
              seed
            else
              power10(ln, seed * 10)
          }

          @inline
          @tailrec
          def renderPositive(ln: Long, power: Long) {
            if (power > 0) {
              val c = Character.forDigit((ln / power % 10).toInt, 10)
              push(c)
              renderPositive(ln, power / 10)
            }
          }

          if (ln == Long.MinValue) {
            val MinString = "-9223372036854775808"
            checkPush(MinString.length)
            buffer.put(MinString)
          } else if (ln == 0) {
            push('0')
          } else if (ln < 0) {
            push('-')

            val ln2 = ln * -1
            renderPositive(ln2, power10(ln2))
          } else {
            renderPositive(ln, power10(ln))
          }
        }

        // TODO is this a problem?
        @inline
        def renderDouble(d: Double) {
          val str = d.toString
          checkPush(str.length)
          buffer.put(str)
        }

        // TODO is this a problem?
        @inline
        def renderNum(d: BigDecimal) {
          val str = d.toString
          checkPush(str.length)
          buffer.put(str)
        }

        @inline
        def renderBoolean(b: Boolean) {
          if (b) {
            pushStr("true")
          } else {
            pushStr("false")
          }
        }

        @inline
        def renderNull() {
          pushStr("null")
        }

        @inline
        def renderEmptyObject() {
          pushStr("{}")
        }

        @inline
        def renderEmptyArray() {
          pushStr("[]")
        }

        @inline
        def renderDate(date: DateTime) {
          renderString(date.toString)
        }

        @inline
        def renderPeriod(period: Period) {
          renderString(period.toString)
        }

        @inline
        def renderArray[A](array: Array[A]) {
          renderString(array.deep.toString)
        }

        def traverseSchema(row: Int, schema: SchemaNode): Boolean = schema match {
          case obj: SchemaNode.Obj => {
            val keys   = obj.keys
            val values = obj.values

            @inline
            @tailrec
            def loop(idx: Int, done: Boolean): Boolean = {
              if (idx < keys.length) {
                val key   = keys(idx)
                val value = values(idx)

                if (done) {
                  pushIn(",", false)
                }

                pushIn(key, true)
                pushIn(":", false)

                val emitted = traverseSchema(row, value)

                if (!emitted) { // less efficient
                  popIn()
                  popIn()

                  if (done) {
                    popIn()
                  }
                }

                loop(idx + 1, done || emitted)
              } else {
                done
              }
            }

            pushIn("{", false)
            val done = loop(0, false)

            if (done) {
              push('}')
            } else {
              popIn()
            }

            done
          }

          case arr: SchemaNode.Arr => {
            val values = arr.nodes

            @inline
            @tailrec
            def loop(idx: Int, done: Boolean): Boolean = {
              if (idx < values.length) {
                val value = values(idx)

                if (done) {
                  pushIn(",", false)
                }

                val emitted = traverseSchema(row, value)

                if (!emitted && done) { // less efficient
                  popIn()
                }

                loop(idx + 1, done || emitted)
              } else {
                done
              }
            }

            pushIn("[", false)
            val done = loop(0, false)

            if (done) {
              push(']')
            } else {
              popIn()
            }

            done
          }

          case union: SchemaNode.Union => {
            val pos = union.possibilities

            @inline
            @tailrec
            def loop(idx: Int): Boolean = {
              if (idx < pos.length) {
                traverseSchema(row, pos(idx)) || loop(idx + 1)
              } else {
                false
              }
            }

            loop(0)
          }

          case SchemaNode.Leaf(tpe, col) =>
            tpe match {
              case CString => {
                val specCol = col.asInstanceOf[StrColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderString(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CBoolean => {
                val specCol = col.asInstanceOf[BoolColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderBoolean(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CLong => {
                val specCol = col.asInstanceOf[LongColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderLong(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CDouble => {
                val specCol = col.asInstanceOf[DoubleColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderDouble(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CNum => {
                val specCol = col.asInstanceOf[NumColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderNum(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CNull => {
                val specCol = col.asInstanceOf[NullColumn]
                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderNull()
                  true
                } else {
                  false
                }
              }

              case CEmptyObject => {
                val specCol = col.asInstanceOf[EmptyObjectColumn]
                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderEmptyObject()
                  true
                } else {
                  false
                }
              }

              case CEmptyArray => {
                val specCol = col.asInstanceOf[EmptyArrayColumn]
                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderEmptyArray()
                  true
                } else {
                  false
                }
              }

              case CDate => {
                val specCol = col.asInstanceOf[DateColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderDate(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CPeriod => {
                val specCol = col.asInstanceOf[PeriodColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderPeriod(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CArrayType(_) => {
                val specCol = col.asInstanceOf[HomogeneousArrayColumn[_]]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  renderArray(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CUndefined => false
            }
        }

        @tailrec
        def render(row: Int, delimit: Boolean): Boolean = {
          if (row < size) {
            if (delimit) {
              pushIn(delimiter, false)
            }

            val rowRendered = traverseSchema(row, schema)

            if (delimit && !rowRendered) {
              popIn()
            }

            render(row + 1, delimit || rowRendered)
          } else {
            delimit
          }
        }

        val rendered = render(0, false)

        buffer.flip()
        vector += buffer

        val stream = StreamT.unfoldM(0) { idx =>
          Need(
            if (idx < vector.length)
              Some((vector(idx), idx + 1))
            else
              None
          )
        }

        (stream, rendered)
      } else emptyStreamT[CharBuffer] -> false
    }
  }

  private sealed trait SchemaNode
  private object SchemaNode {
    final case class Obj(nodes: Map[String, SchemaNode]) extends SchemaNode {
      final var keys: Array[String]       = _
      final var values: Array[SchemaNode] = _
    }

    final case class Arr(map: Map[Int, SchemaNode]) extends SchemaNode {
      final var nodes: Array[SchemaNode] = _
    }

    final case class Union(nodes: Set[SchemaNode]) extends SchemaNode {
      final var possibilities: Array[SchemaNode] = _
    }

    final case class Leaf(tpe: CType, col: Column) extends SchemaNode
  }
}
