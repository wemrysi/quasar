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

package ygg.table

import scala.Predef.$conforms
import ygg.common._
import ColumnMap._

object ColumnMap {
  type Raw = Map[ColumnRef, Column]
  implicit def liftMap(x: Raw): ColumnMap = apply(x.toVector)

  def empty: ColumnMap                                      = Eager(Vector())
  def apply(fields: Vector[ColumnRef -> Column]): ColumnMap = Eager(fields)
  def unapply(x: ColumnMap)                                 = Some(x.fields)

  final case class Eager(fields: Vector[ColumnKV]) extends ColumnMap {
    def mapFields(f: EndoA[Vector[KV]]): Eager = Eager(f(fields))
  }
  final case class Lazy(fieldsFn: () => Vector[ColumnKV]) extends ColumnMap {
    def fields: Vector[KV]                    = fieldsFn()
    def mapFields(f: EndoA[Vector[KV]]): Lazy = Lazy(() => f(fieldsFn()))
  }
}
sealed trait ColumnMap {
  type K  = ColumnRef
  type V  = Column
  type KV = K -> V

  def fields: Vector[KV]
  def mapFields(f: EndoA[Vector[KV]]): ColumnMap

  lazy val asMap: Map[K, V] = fields.toMap

  def ++(that: ColumnMap)                          = mapFields(_ ++ that.fields)
  def +(kv: KV)                                    = mapFields(_ :+ kv)
  def filter(p: KV => Boolean)                     = mapFields(_ filter p)
  def filterKeys(p: K => Boolean)                  = mapFields(_ filter (kv => p(kv._1)))
  def map(f: EndoA[KV])                            = mapFields(_ map f)
  def apply(key: K): V                             = asMap(key)
  def collect[A](pf: KV =?> A): Vector[A]          = fields collect pf
  def collectFields(pf: MaybeSelf[KV]): ColumnMap  = mapFields(_ filter pf.isDefinedAt map pf)
  def exists(p: KV => Boolean): Boolean            = fields exists p
  def flatMap(f: KV => Traversable[KV]): ColumnMap = Eager(fields flatMap f)
  def foldLeft[A](zero: A)(f: (A, KV) => A): A     = fields.foldLeft(zero)(f)
  def foreach(f: KV => Any): Unit                  = fields foreach f
  def get(key: K): Option[V]                       = asMap get key
  def getOrElse(key: K, alt: => V): V              = asMap.getOrElse(key, alt)
  def groupBy[A](f: KV => A): Map[A, ColumnMap]    = fields groupBy f mapValues (vs => Eager(vs))
  def head: KV                                     = fields.head
  def headOption: Option[KV]                       = fields.headOption
  def isEmpty: Boolean                             = fields.isEmpty
  def keySet: Set[K]                               = keys.toSet
  def keys: Vector[K]                              = fields map (_._1)
  def lazyMapValues(f: V => V): Lazy               = lazyColumnMap(fields map { case (k, v) => k -> f(v) })
  def mapValues(f: V => V): Eager                  = columnMap(fields map { case (k, v) => k -> f(v) }: _*)
  def partition(p: KV => Boolean)                  = asMap partition p
  def size: Int                                    = fields.length
  def toArray: Array[KV]                           = fields.toArray
  def toList: List[KV]                             = fields.toList
  def toSeq: Seq[KV]                               = fields
  def unzip: (Vector[K], Vector[V])                = fields.unzip
  def values: Vector[V]                            = fields map (_._2)

  def sortedFields: Vector[KV] = fields sortBy (_._1)
  def pathTraversal            = CPathTraversal(selectors)
  def selectors: Vector[CPath] = keys map (_.selector)
  def types: Vector[CType]     = keys map (_.ctype)
  def isOnePathPerColumn       = selectors.distinct.size == selectors.size
  def containsHomoArray        = types exists { case CArrayType(_) => true ; case _ => false }

  def groupedByPath: Map[CPath, Set[V]] = keys groupBy (_.selector) mapValues (refs => refs.map(apply).toSet) toMap

  def updated(key: K, value: V): ColumnMap = fields indexWhere (kv => key == kv._1) match {
    case -1 => this + (key -> value)
    case n  => mapFields(fs => (fs take n) ++ Vector(key -> value) ++ (fs drop n + 1))
  }

  def column_s: String = (
    fields map {
      case (ColumnRef(path, tpe), column) =>
        "%10s: %-10s => %s".format(path, tpe, column)
    } mkString ("[\n  ", "\n  ", "\n]")
  )
  override def toString = "Columns" + column_s
}
