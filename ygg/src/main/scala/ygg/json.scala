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

import jawn._
import ygg.common._
import scalaz._, Scalaz._
import quasar.Data

package object json {
  type JSchemaElem = ygg.json.JPath -> ygg.table.CType
  type JSchema     = Seq[JSchemaElem]

  type JPathValue   = JPath -> JValue
  type JStringValue = String -> JValue

  type Result[A]    = Validation[Throwable, A]

  val NoJPath = JPath()
  def undef: JValue = JUndefined

  def jvalueToData(x: JValue): Data = x match {
    case JUndefined => Data.NA
    case _          => jawn.Parser.parseUnsafe[Data](x.toString)
  }
  def dataToJValue(d: Data): JValue = {
    import quasar._
    implicit val codec = DataCodec.Precise

    d match {
      case Data.NA => JUndefined
      case _       =>
        (DataCodec render d).fold(
          e => abort(e.toString),
          jv => (JParser parseFromString jv).disjunction.fold(throw _, x => x)
        )
    }
  }

  def toRecord(ids: Array[Long], jv: JValue): JValue = jobject(
    "key"   -> jarray(ids map (x => JNum(x)): _*),
    "value" -> jv
  )

  implicit class AsyncParserOps[A](p: AsyncParser[A])(implicit z: Facade[A]) {
    type R = AsyncParse[A] -> AsyncParser[A]

    def apply(s: String): R       = apply(utf8Bytes(s))
    def apply(bb: Array[Byte]): R = apply(byteBuffer(bb))
    def apply(bb: ByteBuffer): R = p absorb bb match {
      case scala.util.Right(x)                => AsyncParse(Nil, x) -> p.copy()
      case scala.util.Left(t: ParseException) => AsyncParse(Seq(t), Nil) -> p.copy()
      case scala.util.Left(t)                 => AsyncParse(Seq(new ParseException(t.getMessage, 0, 0, 0)), Nil) -> p.copy()
    }
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

  def jarray(elements: JValue*): JValue              = JArray(elements.toVector)
  def jobject(fields: JField*): JValue               = JObject(fields.toList)
  def jfield[A](name: String, value: JValue): JField = JField(name, value)
  def jtrue(): JValue                                = JTrue
  def jfalse(): JValue                               = JFalse

  implicit def liftJPathField(name: String): JPathNode = JPathField(name)
  implicit def liftJPathIndex(index: Int): JPathNode   = JPathIndex(index)
  implicit def liftJPath(path: String): JPath          = JPath(path)

  implicit def JPathNodeOrder: Order[JPathNode] = Order orderBy (x => x.optName -> x.optIndex)
  implicit def JPathOrder: Order[JPath]         = Order orderBy (_.nodes.toList) /* .toList VERY IMPORTANT */

  implicit val JObjectMergeMonoid = new Monoid[JObject] {
    val zero = JObject(Nil)

    def append(v1: JObject, v2: => JObject): JObject = v1.merge(v2).asObject
  }

  private[json] def buildString(f: StringBuilder => Unit): String = {
    val sb = new StringBuilder
    f(sb)
    sb.toString
  }

  implicit class JPathOps(private val x: JPath) {
    import x._

    def parent: Option[JPath]        = if (nodes.isEmpty) None else Some(JPath(nodes dropRight 1: _*))
    def apply(index: Int): JPathNode = nodes(index)
    def head: Option[JPathNode]      = nodes.headOption
    def tail: JPath                  = JPath(nodes.tail)
    def path: String                 = x.to_s

    def ancestors: Vec[JPath] = {
      def loop(path: JPath, acc: Vec[JPath]): Vec[JPath] = path.parent.fold(acc)(p => loop(p, p +: acc))
      loop(x, Vec()).reverse
    }

    def \(that: JPath): JPath   = JPath(nodes ++ that.nodes)
    def \(that: String): JPath  = JPath(nodes :+ JPathField(that))
    def \(that: Int): JPath     = JPath(nodes :+ JPathIndex(that))
    def \:(that: JPath): JPath  = JPath(that.nodes ++ nodes)
    def \:(that: String): JPath = JPath(JPathField(that) +: nodes)
    def \:(that: Int): JPath    = JPath(JPathIndex(that) +: nodes)

    def dropPrefix(p: JPath): Option[JPath] = {
      def remainder(nodes: Seq[JPathNode], toDrop: Seq[JPathNode]): Option[JPath] = {
        nodes match {
          case x +: xs =>
            toDrop match {
              case `x` +: ys => remainder(xs, ys)
              case Seq()     => Some(JPath(nodes: _*))
              case _         => None
            }

          case Seq() =>
            if (toDrop.isEmpty) Some(JPath(nodes: _*))
            else None
        }
      }

      remainder(nodes, p.nodes)
    }
    def extract(jvalue: JValue): JValue = {
      def extract0(path: Seq[JPathNode], d: JValue): JValue = path match {
        case Seq()                   => d
        case JPathField(name) +: tl  => extract0(tl, d \ name)
        case JPathIndex(index) +: tl => extract0(tl, d(index))
      }
      extract0(nodes, jvalue)
    }
    def expand(jvalue: JValue): Seq[JPath] = {
      def expand0(current: Seq[JPathNode], right: Seq[JPathNode], d: JValue): Seq[JPath] = right match {
        case Seq()                          => Seq(JPath(current: _*))
        case (hd @ JPathIndex(index)) +: tl => expand0(current :+ hd, tl, jvalue(index))
        case (hd @ JPathField(name)) +: tl  => expand0(current :+ hd, tl, jvalue \ name)
      }
      expand0(Vec(), nodes, jvalue)
    }
  }

  implicit class JPathNodeOps(private val x: JPathNode) {
    def optName: Option[String] = x match {
      case JPathField(x) => Some(x)
      case _             => None
    }
    def optIndex: Option[Int] = x match {
      case JPathIndex(x) => Some(x)
      case _             => None
    }
    def \(that: JPath)     = JPath(x +: that.nodes)
    def \(that: JPathNode) = JPath(x, that)
  }

  implicit class JObjectOps(private val x: JObject) {
    import x.fields

    def sortedFields: Vector[JField] = fields.toVector.sorted map (kv => JField(kv._1, kv._2)) filterNot (_.isUndefined)
    def get(name: String): JValue    = fields.getOrElse(name, JUndefined)

    def +(field: JField): JObject                           = x.copy(fields = fields + field.toTuple)
    def -(name: String): JObject                            = x.copy(fields = fields - name)
    def partitionField(field: String): (JValue, JObject)    = get(field) -> JObject(fields - field)
    def partition(f: JField => Boolean): (JObject, JObject) = fields partition (x => f(JField(x._1, x._2))) bimap (JObject(_), JObject(_))
  }

  implicit class JNumOps(private val self: JNum) {
    def toBigDecimal: BigDecimal = self match {
      case JNumBigDec(x) => x
      case JNumStr(x)    => BigDecimal(x)
      case JNumLong(x)   => BigDecimal(x)
    }
    def toDouble: Double = self match {
      case JNumBigDec(x) => x.toDouble
      case JNumStr(x)    => x.toDouble
      case JNumLong(x)   => x.toDouble
    }
    def toLong: Long = self match {
      case JNumBigDec(x) => x.toLong
      case JNumStr(x)    => x.toDouble.toLong
      case JNumLong(x)   => x
    }
  }

  private def unflattenArray(elements: Seq[JPathValue]): JArray =
    elements.foldLeft(JArray(Vec()))((arr, t) => arr.set(t._1, t._2).asArray)

  private def unflattenObject(elements: Seq[JPathValue]): JObject =
    elements.foldLeft(JObject(Nil))((obj, t) => obj.set(t._1, t._2).asObject)

  def unflatten(elements: Seq[JPathValue]): JValue = {
    if (elements.isEmpty) JUndefined
    else {
      val sorted = elements.sortBy(_._1)

      val (xp, xv) = sorted.head

      if (xp == NoJPath && sorted.size == 1) xv
      else if (xp.path.startsWith("[")) unflattenArray(sorted)
      else unflattenObject(sorted)
    }
  }

  implicit class JValueOps(private val self: JValue) {
    def diff(other: JValue)                = Diff.diff(self, other)
    def merge(other: JValue): JValue       = Merge.merge(self, other)
    def isDefined                          = self != JUndefined
    def render: String                     = self.toString
    def toOption: Option[JValue]           = if (isDefined) Some(self) else None
    def getOrElse(that: => JValue): JValue = if (isDefined) self else that
    def get(path: JPath): JValue           = path extract self
    def apply(path: JPath): JValue         = path extract self
    def apply(node: JPathNode): JValue     = apply(JPath(node))

    /** XPath-like expression to query JSON fields by name. Matches only fields on
      * next level.
      */
    def \(nameToFind: String): JValue = self match {
      case j @ JObject(fields) => j.get(nameToFind)
      case _                   => JUndefined
    }

    def \?(nameToFind: String): Option[JValue] = (self \ nameToFind).toOption

    def asNum: JNum = self match {
      case x: JNum => x
      case _       => abort(s"Expected JNum but found: ${self.getClass}")
    }
    def asArray: JArray = self match {
      case x: JArray => x
      case _         => abort(s"Expected JArray but found: ${self.getClass}")
    }
    def asObject: JObject = self match {
      case x: JObject => x
      case _         => abort(s"Expected JObject but found: ${self.getClass}")
    }

    /** XPath-like expression to query JSON fields by name. Returns all matching fields.
      */
    def \\(nameToFind: String): JValue = {
      def find(json: JValue): Vector[JValue] = json match {
        case JObject(l) =>
          l.foldLeft(Vector[JValue]()) {
            case (acc, (`nameToFind`, v)) => v +: acc ::: find(v)
            case (acc, (_, v))            => acc ::: find(v)
          }

        case JArray(l) => l flatMap find
        case _         => Vector.empty
      }
      find(self) match {
        case Seq(x) => x
        case x      => JArray(x)
      }
    }

    /** Return nth element from JSON Array.
      */
    def apply(i: Int): JValue = self match {
      case JArray(xs) => xs lift i getOrElse JUndefined
      case _          => JUndefined
    }

    def unsafeInsert(rootPath: JPath, rootValue: JValue): JValue = {
      def rootTarget = self

      def rec(target: JValue, path: JPath, value: JValue): JValue = {
        if ((target == JNull || target == JUndefined) && path == NoJPath) value
        else {
          def arrayInsert(l: Vector[JValue], i: Int, rem: JPath, v: JValue): Vector[JValue] = {
            def update(l: Vector[JValue], j: Int): Vector[JValue] = l match {
              case x +: xs => (if (j == i) rec(x, rem, v) else x) +: update(xs, j + 1)
              case Seq()   => Vec()
            }

            update(l.padTo(i + 1, JUndefined), 0)
          }
          def fail(): Nothing = {
            val msg = s"""
              |JValue insert would overwrite existing data:
              |  $target \\ $path := $value
              |Initial call was
              |  $rootValue \\ $rootPath := $rootValue
              |""".stripMargin.trim
            abort(msg)
          }

          target match {
            case obj @ JObject(fields) =>
              path.nodes match {
                case JPathField(name) +: nodes =>
                  val (child, rest) = obj.partitionField(name)
                  rest + JField(name, rec(child, JPath(nodes), value))

                case JPathIndex(_) +: _ => abort("Objects are not indexed: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
                case Seq()                => fail()
              }

            case arr @ JArray(elements) =>
              path.nodes match {
                case JPathIndex(index) +: nodes => JArray(arrayInsert(elements, index, JPath(nodes), value))
                case JPathField(_) +: _         => abort("Arrays have no fields: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
                case Seq()                        => fail()
              }

            case JNull | JUndefined =>
              path.nodes match {
                case Seq()                => value
                case JPathIndex(_) +: _ => rec(jarray(), path, value)
                case JPathField(_) +: _ => rec(jobject(), path, value)
              }

            case x =>
              // println(s"Target is $x ${x.getClass}")
              fail()
          }
        }
      }

      rec(rootTarget, rootPath, rootValue)
    }

    def set(path: JPath, value: JValue): JValue =
      if (path == NoJPath) value
      else {
        def arraySet(l: Vector[JValue], i: Int, rem: JPath, v: JValue): Vector[JValue] = {
          def update(l: Vector[JValue], j: Int): Vector[JValue] = l match {
            case x +: xs => (if (j == i) x.set(rem, v) else x) +: update(xs, j + 1)
            case Seq()   => Vector.empty
          }

          update(l.padTo(i + 1, JUndefined), 0)
        }

        self match {
          case obj @ JObject(fields) =>
            path.nodes match {
              case JPathField(name) +: nodes =>
                val (child, rest) = obj.partitionField(name)
                rest + JField(name, child.set(JPath(nodes), value))

              case x =>
                abort("Objects are not indexed: attempted to set " + path + " on " + self)
            }

          case arr @ JArray(elements) =>
            path.nodes match {
              case JPathIndex(index) +: nodes =>
                JArray(arraySet(elements, index, JPath(nodes), value))
              case x =>
                abort("Arrays have no fields: attempted to set " + path + " on " + self)
            }

          case _ =>
            path.nodes match {
              case Seq()                => value
              case JPathIndex(_) +: _ => jarray().set(path, value)
              case JPathField(_) +: _ => jobject().set(path, value)
            }
        }
      }

    def delete(path: JPath): Option[JValue] = path.nodes match {
      case JPathField(name) +: xs =>
        self match {
          case JObject.Fields(fields) => Some(
            JObject(fields flatMap {
              case JField(`name`, value) => value.delete(JPath(xs: _*)).fold(Vec[JField]())(v => Vec(JField(name, v)))
              case x                     => Vec(x)
            }: _*)
          )
          case unmodified => Some(unmodified)
        }
      case JPathIndex(idx) +: xs =>
        self match {
          case JArray(elements) => Some(JArray(elements.zipWithIndex.flatMap { case (v, i) => if (i == idx) v.delete(JPath(xs: _*)) else Some(v) }))
          case unmodified       => Some(unmodified)
        }

      case scSeq() => None
    }

    /** Return direct child elements.
      */
    def children: Iterable[JValue] = self match {
      case JObject(fields) => fields.values
      case JArray(l)       => l
      case _               => Vec()
    }

    /** Return a combined value by folding over JSON by applying a function <code>f</code>
      * for each element. The initial value is <code>z</code>.
      */
    def foldDown[A](z: A)(f: (A, JValue) => A): A = foldDownWithPath(z)((acc, p, v) => f(acc, v))

    /** Return a combined value by folding over JSON by applying a function `f`
      * for each element, passing along the path to the elements. The initial
      * value is `z`.
      */
    def foldDownWithPath[A](z: A)(f: (A, JPath, JValue) => A): A = {
      def rec(acc: A, p: JPath, v: JValue): A = {
        val newAcc = f(acc, p, v)

        v match {
          case JObject(l) =>
            l.foldLeft(newAcc) {
              case (acc, field) =>
                rec(acc, p \ field._1, field._2)
            }

          case JArray(l)                                         =>
            l.zipWithIndex.foldLeft(newAcc) { case (a, (e, idx)) => rec(a, p \ idx, e) }

          case _ => newAcc
        }
      }

      rec(z, NoJPath, self)
    }

    /** Return a combined value by folding over JSON by applying a function <code>f</code>
      * for each element. The initial value is <code>z</code>.
      */
    def foldUp[A](z: A)(f: (A, JValue) => A): A = foldUpWithPath(z) { (acc, p, v) =>
      f(acc, v)
    }

    /** Return a combined value by folding over JSON by applying a function `f`
      * for each element, passing along the path to the elements. The initial
      * value is `z`.
      */
    def foldUpWithPath[A](z: A)(f: (A, JPath, JValue) => A): A = {
      def rec(acc: A, p: JPath, v: JValue): A = {
        f(v match {
          case JObject(l) =>
            l.foldLeft(acc) { (acc, field) =>
              rec(acc, p \ field._1, field._2)
            }

          case JArray(l) =>
            l.zipWithIndex.foldLeft(acc) { (a, t) =>
              val (e, idx) = t; rec(a, p \ idx, e)
            }

          case _ => acc
        }, p, v)
      }

      rec(z, NoJPath, self)
    }

    /** Return a new JValue resulting from applying the given function <code>f</code>
      * to each element, moving from the bottom-up.
      */
    def mapUp(f: JValue => JValue): JValue = mapUpWithPath((p, j) => f(j))

    /** Return a new JValue resulting from applying the given function <code>f</code>
      * to each element and its path, moving from the bottom-up.
      */
    def mapUpWithPath(f: (JPath, JValue) => JValue): JValue = {
      def rec(p: JPath, v: JValue): JValue = v match {
        case JObject(l) =>
          f(p, JObject(l.flatMap { f =>
            val v2 = rec(p \ f._1, f._2)
            if (!v2.isDefined) Nil else JField(f._1, v2) +: Nil

          }))

        case JArray(l) =>
          f(
            p,
            JArray(l.zipWithIndex.flatMap(t =>
              rec(p \ t._2, t._1) match {
                case JUndefined => Nil
                case x          => x +: Nil
            })))

        case x => f(p, x)
      }
      rec(NoJPath, self)
    }

    /** Return a new JValue resulting from applying the given function <code>f</code>
      * to each element, moving from the top-down.
      */
    def mapDown(f: JValue => JValue): JValue = mapDownWithPath((p, j) => f(j))

    /** Return a new JValue resulting from applying the given function <code>f</code>
      * to each element and its path, moving from the top-down.
      */
    def mapDownWithPath(f: (JPath, JValue) => JValue): JValue = {
      def rec(p: JPath, v: JValue): JValue = {
        f(p, v) match {
          case JObject(l) =>
            JObject(l.flatMap {
              case (k, v) =>
                val v2 = rec(p \ k, v)
                if (v2 == JUndefined) Nil else JField(k, v2) +: Nil
            })

          case JArray(l) =>
            JArray(l.zipWithIndex flatMap {
              case (e, idx) =>
                rec(p \ idx, e) match {
                  case JUndefined => Nil
                  case x          => x +: Nil
                }
            })

          case x => x
        }
      }

      rec(NoJPath, self)
    }

    /** Return a new JValue resulting from applying the given partial function <code>f</code>
      * to each element in JSON.
      */
    def transform(f: MaybeSelf[JValue]): JValue = mapUp (x => if (f isDefinedAt x) f(x) else x)

    /** Replaces the matched path values with the result of calling the
      * replacer function on the matches. If the path has no values, the
      * method has no effect -- i.e. it is not an error to specify paths
      * which do not exist.
      */
    def replace(target: JPath, replacer: JValue => JValue): JValue = {
      def replace0(target: JPath, j: JValue): JValue = target.nodes match {
        case Seq() => replacer(j)

        case head +: tail =>
          head match {
            case JPathField(name1) =>
              j match {
                case JObject.Fields(fields) =>
                  JObject(fields.map {
                    case JField(name2, value) if (name1 == name2) => JField(name1, replace0(JPath(tail: _*), value))

                    case field => field
                  })

                case jvalue => jvalue
              }

            case JPathIndex(index) =>
              j match {
                case JArray(elements) =>
                  val split  = elements.splitAt(index)
                  val prefix = split._1
                  val middle = replace0(JPath(tail: _*), split._2.head)
                  val suffix = split._2.drop(1)

                  JArray(prefix ++ (middle +: suffix))

                case jvalue => jvalue
              }
          }
      }

      target.expand(self).foldLeft(self) { (jvalue, expansion) =>
        replace0(expansion, jvalue)
      }
    }

    /** A shorthand for the other replacement in the case that the replacement
      * does not depend on the value being replaced.
      */
    def replace(target: JPath, replacement: JValue): JValue = replace(target, r => replacement)

    /** Return the first element from JSON which matches the given predicate.
      */
    def find(p: JValue => Boolean): Option[JValue] = {
      def find(json: JValue): Option[JValue] = {
        if (p(json)) Some(json)
        else {
          json match {
            case JObject(l) => l.flatMap({ case (_, v) => find(v) }).headOption
            case JArray(l)  => l.flatMap(find).headOption
            case _          => None
          }
        }
      }

      find(self)
    }

    /** Return a List of all elements which matches the given predicate.
      */
    def filter(p: JValue => Boolean): Vector[JValue] =
      foldDown(Vector[JValue]())((acc, e) => if (p(e)) e +: acc else acc).reverse

    def withFilter(p: JValue => Boolean): Vector[JValue] = filter(p)

    def flatten: Vector[JValue] =
      foldDown(Vector[JValue]())((acc, e) => e +: acc).reverse

    /** Flattens the JValue down to a list of path to simple JValue primitive.
      */
    def flattenWithPath: Vector[JPathValue] = {
      def flatten0(path: JPath)(value: JValue): Vector[JPathValue] = value match {
        case JObject.empty | JArray.empty => Vector(path -> value)
        case JObject(fields)              => fields.flatMap({ case (k, v) => flatten0(path \ k)(v) })(breakOut)
        case JArray(elements)             => elements.zipWithIndex.flatMap({ case (element, index) => flatten0(path \ index)(element) })
        case JUndefined                   => Vector()
        case leaf                         => Vector(path -> leaf)
      }

      flatten0(NoJPath)(self)
    }

    /** Concatenate with another JSON.
      * This is a concatenation monoid: (JValue, ++, JUndefined)
      */
    def ++(other: JValue): JValue = {
      def append(value1: JValue, value2: JValue): JValue = (value1, value2) match {
        case (JUndefined, x)          => x
        case (x, JUndefined)          => x
        case (JArray(xs), JArray(ys)) => JArray(xs ++ ys)
        case (JArray(xs), v: JValue)  => JArray(xs :+ v)
        case (v: JValue, JArray(xs))  => JArray(v +: xs)
        case (x, y)                   => jarray(x, y)
      }
      append(self, other)
    }

    /** Return a JSON where all elements matching the given predicate are removed.
      */
    def remove(p: JValue => Boolean): JValue = self mapUp {
      case x if p(x) => JUndefined
      case x         => x
    }
  }
}
