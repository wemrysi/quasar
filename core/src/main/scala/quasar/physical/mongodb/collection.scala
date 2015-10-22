/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{RenderTree, Terminal}
import quasar.fp._
import quasar.fs._, Path._

import scala.util.parsing.combinator._

import scalaz._, Scalaz._
import pathy.Path._

final case class Collection(databaseName: String, collectionName: String) {
  import Collection._

  def asPath: Path = {
    val first = DatabaseNameUnparser(databaseName)
    val rest = CollectionNameUnparser(collectionName)
    val segs = NonEmptyList(first, rest: _*)
    Path(DirNode.Current :: segs.list.dropRight(1).map(DirNode(_)), Some(FileNode(segs.last)))
  }
}
object Collection {
  import PathError._

  // TODO: Remove once Path has been replaced by pathy.Path
  def foldPath[A](path: Path)(clusterF: => A, dbF: String => A, collF: Collection => A):
      PathError \/ A = {
    val abs = path.asAbsolute
    val segs = abs.dir.map(_.value) ++ abs.file.map(_.value).toList

    (segs match {
      case Nil => clusterF.right
      case first :: rest => for {
        db       <- DatabaseNameParser(first)
        collSegs <- rest.traverseU(CollectionSegmentParser(_))
        coll     =  collSegs.mkString(".")
        _        <- if (utf8length(db) + 1 + utf8length(coll) > 120)
                      s"database/collection name too long (> 120 bytes): $db.$coll".left
                    else ().right
      } yield if (collSegs.isEmpty) dbF(db) else collF(Collection(db, coll))
    }) leftMap InvalidPathError
  }

  def fromPath(path: Path): PathError \/ Collection =
    foldPath(path)(
      -\/(PathTypeError(path, Some("has no segments"))),
      κ(-\/(InvalidPathError("path names a database, but no collection: " + path))),
      \/-(_)).join

  def fromFile(f: RelFile[Sandboxed]): PathError2 \/ Collection = {
    import PathError2._

    flatten(None, None, None, Some(_), Some(_), f)
      // NB: We know there is at least one file case, but don't have a way to
      //     prove the the result of `unite` will be non-empty.
      .unite.uncons(scala.sys.error("impossible"), (h, t) =>
        t.toNel.cata(
          ss => (for {
              db   <- DatabaseNameParser(h)
              segs <- ss.traverseU(CollectionSegmentParser(_))
              coll =  segs.toList mkString "."
              len  =  utf8length(db) + 1 + utf8length(coll)
              _    <- if (len > 120)
                        s"database/collection name too long ($len > 120 bytes): $db.$coll".left
                      else ().right
            } yield Collection(db, coll)) leftMap (InvalidPath(f.right, _)),
          InvalidPath(f.right, "no database specified").left))
  }

  private trait PathParser extends RegexParsers {
    override def skipWhitespace = false

    protected def substitute(pairs: List[(String, String)]): Parser[String] =
      pairs.foldLeft[Parser[String]](failure("no match")) {
        case (acc, (a, b)) => (a ^^ κ(b)) | acc
      }
  }

  def utf8length(str: String) = str.getBytes("UTF-8").length

  val DatabaseNameEscapes = List(
    " "  -> "+",
    "."  -> "~",
    "$"  -> "$$",
    "+"  -> "$add",
    "~"  -> "$tilde",
    "/"  -> "$div",
    "\\" -> "$esc",
    "\"" -> "$quot",
    "*"  -> "$mul",
    "<"  -> "$lt",
    ">"  -> "$gt",
    ":"  -> "$colon",
    "|"  -> "$bar",
    "?"  -> "$qmark")

  private object DatabaseNameParser extends PathParser {
    def name: Parser[String] =
      char.* ^^ { _.mkString }

    def char: Parser[String] = substitute(DatabaseNameEscapes) | "(?s).".r

    def apply(input: String): String \/ String = parseAll(name, input) match {
      case Success(name, _) if utf8length(name) > 64 =>
        s"database name too long (> 64 bytes): $name".left
      case Success(name, _) =>
        name.right
      case failure : NoSuccess =>
        s"failed to parse ‘$input’: ${failure.msg}".left
    }
  }

  private object DatabaseNameUnparser extends PathParser {
    def name = nameChar.* ^^ { _.mkString }

    def nameChar = substitute(DatabaseNameEscapes.map(_.swap)) | "(?s).".r

    def apply(input: String): String = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  val CollectionNameEscapes = List(
    "."  -> "\\.",
    "$"  -> "\\d",
    "\\" -> "\\\\")

  private object CollectionSegmentParser extends PathParser {
    def seg: Parser[String] =
      char.* ^^ { _.mkString }

    def char: Parser[String] = substitute(CollectionNameEscapes) | "(?s).".r

    def apply(input: String): String \/ String = parseAll(seg, input) match {
      case Success(seg, _) =>
        seg.right
      case failure : NoSuccess =>
        s"failed to parse ‘$input’: ${failure.msg}".left
    }
  }

  private object CollectionNameUnparser extends PathParser {
    def name = repsep(seg, ".")

    def seg = segChar.* ^^ { _.mkString }

    def segChar = substitute(CollectionNameEscapes.map(_.swap)) | "(?s)[^.]".r

    def apply(input: String): List[String] = parseAll(name, input) match {
      case Success(result, _) => result
      case failure : NoSuccess => scala.sys.error("doesn't happen")
    }
  }

  implicit val CollectionRenderTree = new RenderTree[Collection] {
    def render(v: Collection) = Terminal(List("Collection"), Some(v.databaseName + "; " + v.collectionName))
  }
}
