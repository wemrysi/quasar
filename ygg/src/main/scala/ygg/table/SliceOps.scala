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

import ygg._, common._, json._, data._
import scalaz.{ =?> => _, _ }, Ordering._
import scala.math.min

class SliceOps(private val source: Slice) extends AnyVal {
  import Slice._

  def columns: ColumnMap      = source.columns
  def definedAt: BitSet       = doto(Bits())(defined => columns foreach { case (_, col) => defined or col.definedAt(0, size) })
  def isDefinedAt(row: RowId) = columns.values.exists(_.isDefinedAt(row))
  def isEmpty: Boolean        = size == 0
  def nonEmpty                = !isEmpty
  def size: Int               = source.size

  def dereferencedColumns(derefBy: Int =?> CPathNode): ColumnMap = {
    val forwardIndex: Map[CPathNode, ColumnMap.Raw] = columns.foldLeft(Map.empty[CPathNode, ColumnMap.Raw]) {
      case (acc, (ColumnRef(CPath(root, xs @ _ *), ctype), col)) =>
        val resultRef = ColumnRef(CPath(xs: _*), ctype)
        // we know the combination of xs and ctype to be unique within root
        acc + (root -> (acc.getOrElse(root, Map()) + (resultRef -> col)))
    }

    val indexableArrays: Map[ColumnRef, HomogeneousArrayColumn[_]] = columns.fields.collect {
      case (ColumnRef(CPath(CPathArray, xs @ _ *), ctype), col: HomogeneousArrayColumn[_]) =>
        (ColumnRef(CPath(xs: _*), ctype), col)
    }.toMap

    def derefColumns(node: CPathNode): Option[ColumnMap.Raw] = node match {
      case CPathIndex(i) if !indexableArrays.isEmpty =>
        Some((indexableArrays mapValues (_.select(i))) ++ forwardIndex.getOrElse(node, Map.empty))
      case _ => forwardIndex get node
    }

    ColumnMap.Eager(columns.keySet.foldLeft(Map.empty[ColumnRef, Column]) {
      case (acc, ColumnRef(CPath(_, xs @ _ *), ctype)) =>
        val resultRef = ColumnRef(CPath(xs: _*), ctype)

        lazy val resultCol = ctype match {
          case CBoolean =>
            new BoolColumn {
              private var row0: Int           = -1
              private var refCol0: BoolColumn = _
              @inline private def refCol(row: Int): BoolColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[BoolColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CLong =>
            new LongColumn {
              private var row0: Int           = -1
              private var refCol0: LongColumn = _
              @inline private def refCol(row: Int): LongColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[LongColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CDouble =>
            new DoubleColumn {
              private var row0: Int             = -1
              private var refCol0: DoubleColumn = _
              @inline private def refCol(row: Int): DoubleColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[DoubleColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CNum =>
            new NumColumn {
              private var row0: Int          = -1
              private var refCol0: NumColumn = _
              @inline private def refCol(row: Int): NumColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[NumColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CString =>
            new StrColumn {
              private var row0: Int          = -1
              private var refCol0: StrColumn = _
              @inline private def refCol(row: Int): StrColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[StrColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CDate =>
            new DateColumn {
              private var row0: Int           = -1
              private var refCol0: DateColumn = _
              @inline private def refCol(row: Int): DateColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[DateColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CPeriod =>
            new PeriodColumn {
              private var row0: Int             = -1
              private var refCol0: PeriodColumn = _
              @inline private def refCol(row: Int): PeriodColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[PeriodColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case cArrayType: CArrayType[a] =>
            new HomogeneousArrayColumn[a] {
              val tpe                                        = cArrayType
              private var row0: Int                          = -1
              private var refCol0: HomogeneousArrayColumn[a] = _
              @inline private def refCol(row: Int): HomogeneousArrayColumn[a] =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[HomogeneousArrayColumn[a]]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CEmptyObject =>
            new EmptyObjectColumn {
              def isDefinedAt(row: Int) =
                derefBy.isDefinedAt(row) &&
                  derefColumns(derefBy(row)).exists(_.get(resultRef).exists(_.isDefinedAt(row)))
            }

          case CEmptyArray =>
            new EmptyArrayColumn {
              def isDefinedAt(row: Int) =
                derefBy.isDefinedAt(row) &&
                  derefColumns(derefBy(row)).exists(_.get(resultRef).exists(_.isDefinedAt(row)))
            }

          case CNull =>
            new NullColumn {
              def isDefinedAt(row: Int) = derefBy.isDefinedAt(row) && derefColumns(derefBy(row)).exists(cols => cols(resultRef).isDefinedAt(row))
            }

          case CUndefined => UndefinedColumn.raw
        }

        acc + (resultRef -> acc.getOrElse(resultRef, resultCol))
    }.toVector)
  }

  def lazyMapColumns(f: EndoA[Column]): ColumnMap = columns lazyMapValues f

  def logicalColumns: JType => Set[Column] = { jtpe =>
    // TODO Use a flatMap and:
    // If ColumnRef(_, CArrayType(_)) and jType has a JArrayFixedT of this type,
    //   then we need to map these to multiple columns.
    // Else if Schema.includes(...), then return List(col).
    // Otherwise return Nil.
    columns collect {
      case (ColumnRef(cpath, ctype), col) if Schema.includes(jtpe, cpath, ctype) => col
    } toSet
  }

  def mapRoot(f: CF1): Slice =
    Slice(source.size, {
      val resultColumns = for {
        col    <- columns.fields collect { case (ref, col) if ref.selector == CPath.Identity => col }
        result <- f(col)
      } yield result

      resultColumns.groupBy(_.tpe) map {
        case (tpe, cols) => (ColumnRef.id(tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
      }
    })

  def mapColumns(f: CF1): Slice =
    Slice(
      source.size,
      ColumnMap.Eager({
        val resultColumns = columns.fields flatMap {
          case (ref, col) => f(col) map (result => ref.copy(ctype = result.tpe) -> result)
        }

        resultColumns.groupBy(_._1).toVector map {
          case (ref, pairs) => (ref, pairs.map(_._2).reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
        }
      })
    )

  def toArray[A](implicit tpe0: CValueType[A]): Slice = {
    val cols0 = (columns).toList sortBy { case (ref, _) => ref.selector }
    val cols  = cols0 map { case (_, col)                      => col }

    def inflate[@spec A: CTag](cols: Array[Int => A], row: RowId) = {
      val as = new Array[A](cols.length)
      var i  = 0
      while (i < cols.length) {
        as(i) = cols(i)(row)
        i += 1
      }
      as
    }

    def loopForall[A <: Column](cols: Array[A])(row: RowId) = !cols.isEmpty && Loop.forall(cols)(_ isDefinedAt row)

    def rhs = tpe0 match {
      case CLong =>
        val longcols = cols.collect { case (col: LongColumn) => col }.toArray

        new HomogeneousArrayColumn[Long] {
          private val cols: Array[Int => Long] = longcols map (col => col apply _)

          val tpe                          = CArrayType(CLong)
          def isDefinedAt(row: RowId)        = loopForall[LongColumn](longcols)(row)
          def apply(row: RowId): Array[Long] = inflate(cols, row)
        }
      case CDouble =>
        val doublecols = cols.collect { case (col: DoubleColumn) => col }.toArray
        new HomogeneousArrayColumn[Double] {
          private val cols: Array[Int => Double] = doublecols map (col => col apply _)

          val tpe                            = CArrayType(CDouble)
          def isDefinedAt(row: RowId)          = loopForall[DoubleColumn](doublecols)(row)
          def apply(row: RowId): Array[Double] = inflate(cols, row)
        }
      case CNum =>
        val numcols = cols.collect { case (col: NumColumn) => col }.toArray
        new HomogeneousArrayColumn[BigDecimal] {
          private val cols: Array[Int => BigDecimal] = numcols map (col => col apply _)

          val tpe                                = CArrayType(CNum)
          def isDefinedAt(row: RowId)              = loopForall[NumColumn](numcols)(row)
          def apply(row: RowId): Array[BigDecimal] = inflate(cols, row)
        }
      case CBoolean =>
        val boolcols = cols.collect { case (col: BoolColumn) => col }.toArray
        new HomogeneousArrayColumn[Boolean] {
          private val cols: Array[Int => Boolean] = boolcols map (col => col apply _)

          val tpe                             = CArrayType(CBoolean)
          def isDefinedAt(row: RowId)           = loopForall[BoolColumn](boolcols)(row)
          def apply(row: RowId): Array[Boolean] = inflate(cols, row)
        }
      case CString =>
        val strcols = cols.collect { case (col: StrColumn) => col }.toArray
        new HomogeneousArrayColumn[String] {
          private val cols: Array[Int => String] = strcols map (col => col apply _)

          val tpe                            = CArrayType(CString)
          def isDefinedAt(row: RowId)          = loopForall[StrColumn](strcols)(row)
          def apply(row: RowId): Array[String] = inflate(cols, row)
        }
      case _ => abort("unsupported type")
    }

    def key = ColumnRef(CPath(CPathArray), CArrayType(tpe0))
    Slice(source.size, Map(key -> rhs))
  }

  /**
    * Transform this slice such that its columns are only defined for row indices
    * in the given BitSet.
    */
  def redefineWith(s: BitSet): Slice = mapColumns(cf.filter(0, size, s))

  def definedConst(value: CValue): Slice =
    Slice(source.size, Map(value match {
      case CString(s) =>
        (ColumnRef.id(CString), new StrColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
          def apply(row: RowId)       = s
        })
      case CBoolean(b) =>
        (ColumnRef.id(CBoolean), new BoolColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
          def apply(row: RowId)       = b
        })
      case CLong(l) =>
        (ColumnRef.id(CLong), new LongColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
          def apply(row: RowId)       = l
        })
      case CDouble(d) =>
        (ColumnRef.id(CDouble), new DoubleColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
          def apply(row: RowId)       = d
        })
      case CNum(n) =>
        (ColumnRef.id(CNum), new NumColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
          def apply(row: RowId)       = n
        })
      case CDate(d) =>
        (ColumnRef.id(CDate), new DateColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
          def apply(row: RowId)       = d
        })
      case CPeriod(p) =>
        (ColumnRef.id(CPeriod), new PeriodColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
          def apply(row: RowId)       = p
        })
      case value: CArray[a] =>
        (ColumnRef.id(value.cType), new HomogeneousArrayColumn[a] {
          val tpe                     = value.cType.asInstanceOf[CArrayType[a]]
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
          def apply(row: RowId)       = value.value
        })
      case CNull =>
        (ColumnRef.id(CNull), new NullColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
        })
      case CEmptyObject =>
        (ColumnRef.id(CEmptyObject), new EmptyObjectColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
        })
      case CEmptyArray =>
        (ColumnRef.id(CEmptyArray), new EmptyArrayColumn {
          def isDefinedAt(row: RowId) = source.isDefinedAt(row)
        })
      case CUndefined => abort("Cannot define a constant undefined value")
    }))

  def deref(node: CPathNode): Slice = Slice(
    source.size,
    ColumnMap.Eager(node match {
      case CPathIndex(i) =>
        columns.fields collect {
          case (ColumnRef(CPath(CPathArray, xs @ _ *), CArrayType(elemType)), col: HomogeneousArrayColumn[_]) =>
            ColumnRef(CPath(xs: _*), elemType) -> col.select(i)

          case (ColumnRef(CPath(CPathIndex(`i`), xs @ _ *), ctype), col) =>
            ColumnRef(CPath(xs: _*), ctype) -> col
        }

      case _ =>
        columns.fields collect {
          case (ColumnRef(CPath(`node`, xs @ _ *), ctype), col) =>
            ColumnRef(CPath(xs: _*), ctype) -> col
        }
    })
  )
  def deref(i: Int): Slice             = deref(CPathIndex(i))
  def deref(name: scala.Symbol): Slice = deref(CPathField(name.name))

  def wrap(wrapper: CPathNode): Slice = Slice(
    source.size,
    // This is a little weird; CPathArray actually wraps in CPathIndex(0).
    // Unfortunately, CArrayType(_) cannot wrap CNullTypes, so we can't just
    // arbitrarily wrap everything in a CPathArray.
    ColumnMap.Eager(wrapper match {
      case CPathArray =>
        columns.fields map {
          case (ColumnRef(CPath(nodes @ _ *), ctype), col) =>
            ColumnRef(CPath(CPathIndex(0) +: nodes: _*), ctype) -> col
        }
      case _ =>
        columns.fields map {
          case (ColumnRef(CPath(nodes @ _ *), ctype), col) =>
            ColumnRef(CPath(wrapper +: nodes: _*), ctype) -> col
        }
    })
  )

  // ARRAYS:
  // TODO Here, if we delete a JPathIndex/JArrayFixedT, then we need to
  // construct a new Homo*ArrayColumn that has some indices missing.
  //
  // -- I've added a col.without(indicies) method to H*ArrayColumn to support
  // this operation.
  //
  def delete(jtype: JType): Slice = {
    def fixArrays(columns: ColumnMap): ColumnMap = {
      columns.toSeq
        .sortBy(_._1)
        .foldLeft((Map.empty[Vector[CPathNode], Int], Map.empty[ColumnRef, Column])) {
          case ((arrayPaths, acc), (ColumnRef(jpath, ctype), col)) =>
            val (arrayPaths0, nodes) = jpath.nodes.foldLeft((arrayPaths, Vector.empty[CPathNode])) {
              case ((ap, nodes), CPathIndex(_)) =>
                val idx = ap.getOrElse(nodes, -1) + 1
                (ap + (nodes -> idx), nodes :+ CPathIndex(idx))

              case ((ap, nodes), fieldNode) => (ap, nodes :+ fieldNode)
            }

            (arrayPaths0, acc + (ColumnRef(CPath(nodes: _*), ctype) -> col))
        }
        ._2
    }

    // Used for homogeneous arrays. Constructs a function, suitable for use in a
    // flatMap, that will modify the homogeneous array according to `jType`.
    //
    def flattenDeleteTree[A](jType: JType, cType: CValueType[A], cPath: CPath): A => Option[A] = {
      val delete: A => Option[A] = _ => None
      val retain: A => Option[A] = Some(_)

      (jType, cType, cPath) match {
        case (JUnionT(aJType, bJType), _, _) =>
          flattenDeleteTree(aJType, cType, cPath) andThen (_ flatMap flattenDeleteTree(bJType, cType, cPath))
        case (JTextT, CString, CPath.Identity) =>
          delete
        case (JBooleanT, CBoolean, CPath.Identity) =>
          delete
        case (JNumberT, CLong | CDouble | CNum, CPath.Identity) =>
          delete
        case (JObjectUnfixedT, _, CPath(CPathField(_), _ *)) =>
          delete
        case (JObjectFixedT(fields), _, CPath(CPathField(name), cPath @ _ *)) =>
          fields get name map (flattenDeleteTree(_, cType, CPath(cPath: _*))) getOrElse (retain)
        case (JArrayUnfixedT, _, CPath(CPathArray | CPathIndex(_), _ *)) =>
          delete
        case (JArrayFixedT(elems), cType, CPath(CPathIndex(i), cPath @ _ *)) =>
          elems get i map (flattenDeleteTree(_, cType, CPath(cPath: _*))) getOrElse (retain)
        case (JArrayFixedT(elems), CArrayType(cElemType), CPath(CPathArray, cPath @ _ *)) =>
          val mappers = elems mapValues (flattenDeleteTree(_, cElemType, CPath(cPath: _*)))
          xs =>
            Some(xs.zipWithIndex map {
              case (x, j) =>
                mappers get j match {
                  case Some(f) => f(x)
                  case None    => x
                }
            })
        case (JArrayHomogeneousT(jType), CArrayType(cType), CPath(CPathArray, _ *)) if Schema.ctypes(jType)(cType) =>
          delete
        case _ =>
          retain
      }
    }

    Slice(
      source.size,
      fixArrays(columns flatMap {
        case (ColumnRef(cpath, ctype), _) if Schema.includes(jtype, cpath, ctype) => None

        case (ref @ ColumnRef(cpath, ctype: CArrayType[a]), col: HomogeneousArrayColumn[_]) if ctype == col.tpe =>
          val trans = flattenDeleteTree(jtype, ctype, cpath)
          Some((ref, new HomogeneousArrayColumn[a] {
            val tpe                         = ctype
            def isDefinedAt(row: RowId)     = col.isDefinedAt(row)
            def apply(row: RowId): Array[a] = trans(col(row).asInstanceOf[Array[a]]) getOrElse abort("Oh dear, this cannot be happening to me.")
          }))

        case (ref, col) => Some(ref -> col)
      })
    )
  }

  def deleteFields(prefixes: Set[CPathField]): Slice = {
    val (removed, withoutPrefixes) = columns partition {
      case (ColumnRef.head(hd: CPathField), _) => prefixes contains hd
      case _                                   => false
    }

    val becomeEmpty = Bits.filteredRange(0, source.size) { i =>
      Column.isDefinedAt(removed.values.toArray, i) && !Column.isDefinedAt(withoutPrefixes.values.toArray, i)
    }

    val ref = ColumnRef.id(CEmptyObject)

    // The object might have become empty. Make the
    // EmptyObjectColumn defined at the row position.
    lazy val emptyObjectColumn = withoutPrefixes get ref map { c =>
      new EmptyObjectColumn {
        def isDefinedAt(row: RowId) = c.isDefinedAt(row) || becomeEmpty(row)
      }
    } getOrElse {
      new EmptyObjectColumn {
        def isDefinedAt(row: RowId) = becomeEmpty(row)
      }
    }

    if (becomeEmpty.isEmpty)
      Slice(size, withoutPrefixes)
    else
      Slice(size, withoutPrefixes + (ref -> emptyObjectColumn))
  }

  def typed(jtpe: JType): Slice =
    Slice(size, columns filterKeys (Schema.requiredBy(jtpe, _)))

  def typedSubsumes(jtpe: JType): Slice = {
    val columns = ColumnMap.Eager(
      source.columns.fields |> (kvs =>
        if (Schema.subsumes(jtpe, kvs map (_._1))) kvs filter (kv => Schema.requiredBy(jtpe, kv._1))
        else Vector()
      )
    )

    Slice(source.size, columns)
  }

  /**
    * returns a BoolColumn that is true if row subsumes jtype, false otherwise (unless undefined)
    * determine if the supplied jtype subsumes all the columns
    * if false, return a BoolColumn with all falses, defined by union
    * if true, collect just those columns that the jtype specifies
    * then on a row-by-row basis, using a BitSet, we use `Schema.findTypes(...)` to determine the Boolean values
    */
  def isType(jtpe: JType): Slice = {
    val pathsAndTypes: Seq[CPath -> CType] = columns.toSeq map { case (ColumnRef(selector, ctype), _) => (selector, ctype) }
    // we cannot just use subsumes because there could be rows with undefineds in them
    val subsumes    = Schema.subsumes(pathsAndTypes, jtpe)
    val definedBits = (columns).values.map(_.definedAt(0, size)).reduceOption(_ | _) getOrElse new BitSet

    def mapValue = (
      if (subsumes) {
        val cols         = columns filter { case (ColumnRef(path, ctpe), _) => Schema.requiredBy(jtpe, path, ctpe) }
        val included     = Schema.findTypes(jtpe, CPath.Identity, cols, size)
        val includedBits = Bits.filteredRange(0, size)(included)

        BoolColumn.Either(definedBits, includedBits)
      } else BoolColumn.False(definedBits)
    )

    Slice(source.size, Map(ColumnRef.id(CBoolean) -> mapValue))
  }

  def arraySwap(index: Int): Slice =
    Slice(
      source.size,
      ColumnMap.Eager(columns.fields collect {
      case (ColumnRef(cPath @ CPath(CPathArray, _ *), cType), col: HomogeneousArrayColumn[a]) =>
        (ColumnRef(cPath, cType), new HomogeneousArrayColumn[a] {
          val tpe                   = col.tpe
          def isDefinedAt(row: RowId) = col.isDefinedAt(row)
          def apply(row: RowId) = {
            val xs = col(row)
            if (index >= xs.length) xs
            else {
              val ys = tpe.elemType.classTag.newArray(xs.length)

              var i = 1
              while (i < ys.length) {
                ys(i) = xs(i)
                i += 1
              }

              ys(0) = xs(index)
              ys(index) = xs(0)
              ys
            }
          }
        })

      case (ColumnRef(CPath(CPathIndex(0), xs @ _ *), ctype), col) =>
        (ColumnRef(CPath(CPathIndex(index) +: xs: _*), ctype), col)

      case (ColumnRef(CPath(CPathIndex(`index`), xs @ _ *), ctype), col) =>
        (ColumnRef(CPath(CPathIndex(0) +: xs: _*), ctype), col)

      case c @ (ColumnRef(CPath(CPathIndex(i), xs @ _ *), ctype), col) => c
    }))

  // Takes an array where the indices correspond to indices in this slice,
  // and the values give the indices in the sparsened slice.
  def sparsen(index: Array[Int], toSize: Int): Slice = Slice(
    toSize,
    lazyMapColumns(col => cf.Sparsen(index, toSize)(col).get) //sparsen is total
  )
  def remap(indices: ArrayIntList): Slice = Slice(
    indices.size,
    lazyMapColumns(col => cf.RemapIndices(indices).apply(col).get)
  )

  def map(from: CPath, to: CPath)(f: CF1): Slice = {
    val resultColumns = for {
      col    <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(from) => col }
      result <- f(col)
    } yield result

    val columns: ColumnMap = {
      resultColumns.groupBy(_.tpe) map {
        case (tpe, cols) => (ColumnRef(to, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
      }
    }

    Slice(source.size, columns)
  }

  def map2(froml: CPath, fromr: CPath, to: CPath)(f: CF2): Slice = Slice(
    source.size, {
      val resultColumns = for {
        left   <- columns collect { case (ref, col) if ref.selector.hasPrefix(froml) => col }
        right  <- columns collect { case (ref, col) if ref.selector.hasPrefix(fromr) => col }
        result <- f(left, right)
      } yield result

      resultColumns.groupBy(_.tpe) map { case (tpe, cols) => (ColumnRef(to, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2))) }
    }
  )

  def filterDefined(filter: Slice, definedness: Definedness): Slice = {
    val colValues = filter.columns.values.toArray
    lazy val defined = definedness match {
      case AnyDefined =>
        Bits.filteredRange(0, source.size) { i =>
          colValues.exists(_.isDefinedAt(i))
        }

      case AllDefined =>
        if (colValues.isEmpty)
          new BitSet
        else
          Bits.filteredRange(0, source.size) { i =>
            colValues.forall(_.isDefinedAt(i))
          }
    }

    Slice(source.size, lazyMapColumns(col => cf.filter(0, source.size, defined)(col).get))
  }

  def compact(filter: Slice, definedness: Definedness): Slice = {
    val cols = filter.columns
    val retained = definedness match {
      case AnyDefined =>
        doto(new ArrayIntList) { acc =>
          0 until filter.size foreach (i => if (cols.values exists (_ isDefinedAt i)) discard(acc.add(i)))
        }

      case AllDefined =>
        doto(new ArrayIntList) { acc =>
          val (numCols, otherCols) = cols partition {
            case (ColumnRef(_, _: CNumericType[_]), _) => true
            case (ColumnRef(_, _), _)                  => false
          }
          val grouped = numCols groupBy (_._1.selector)

          0 until filter.size foreach { i =>
            def numBools  = grouped.values map (_.values.toArray exists (_ isDefinedAt i))
            def numBool   = numBools reduce (_ && _)
            def otherBool = otherCols.values.toArray forall (_ isDefinedAt i)

            if (otherBool && numBool)
              discard(acc.add(i))
          }
        }
    }

    Slice(retained.size, lazyMapColumns(_ |> cf.RemapIndices(retained) get))
  }

  def retain(refs: Set[ColumnRef]): Slice = Slice(source.size, columns filterKeys refs) // !!! filterKeys is on-demand

  /**
    * Assumes that this and the previous slice (if any) are sorted.
    */
  def distinct(prevFilter: Option[Slice], filter: Slice): Slice = {
    lazy val retained: ArrayIntList = {
      val acc = new ArrayIntList

      def findSelfDistinct(prevRow: Int, curRow: Int) = {
        val selfComparator = rowComparatorFor(filter, filter)(_.columns.keys map (_.selector))

        @tailrec
        def findSelfDistinct0(prevRow: Int, curRow: Int): ArrayIntList = {
          if (curRow >= filter.size) acc
          else {
            val retain = selfComparator.compare(prevRow, curRow) != EQ
            if (retain) acc.add(curRow)
            findSelfDistinct0(if (retain) curRow else prevRow, curRow + 1)
          }
        }

        findSelfDistinct0(prevRow, curRow)
      }

      def findStraddlingDistinct(prev: Slice, prevRow: Int, curRow: Int) = {
        val straddleComparator = rowComparatorFor(prev, filter)(_.columns.keys map (_.selector))

        @tailrec
        def findStraddlingDistinct0(prevRow: Int, curRow: Int): ArrayIntList = {
          if (curRow >= filter.size) acc
          else {
            val retain = straddleComparator.compare(prevRow, curRow) != EQ
            if (retain) acc.add(curRow)
            if (retain)
              findSelfDistinct(curRow, curRow + 1)
            else
              findStraddlingDistinct0(prevRow, curRow + 1)
          }
        }

        findStraddlingDistinct0(prevRow, curRow)
      }

      val lastDefined = prevFilter.flatMap { slice =>
        (slice.size - 1 to 0 by -1).find(row => slice.columns.values.exists(_.isDefinedAt(row)))
      }.map {
        (prevFilter.get, _)
      }

      val firstDefined = (0 until filter.size).find(i => filter.columns.values.exists(_.isDefinedAt(i)))

      (lastDefined, firstDefined) match {
        case (Some((prev, i)), Some(j)) => findStraddlingDistinct(prev, i, j)
        case (_, Some(j))               => acc.add(j); findSelfDistinct(j, j + 1)
        case _                          => acc
      }
    }

    Slice(retained.size, lazyMapColumns(_ |> cf.RemapIndices(retained) get))
  }

  def order: Ord[Int] =
    if (columns.size == 1) {
      val col = columns.head._2
      Column.rowOrder(col)
    } else {

      // The 2 cases are handled differently. In the first case, we don't have
      // any pesky homogeneous arrays and only 1 column per path. In this case,
      // we don't need to use the CPathTraversal machinery.

      type GroupedCols = Either[Map[CPath, Column], Map[CPath, Set[Column]]]

      val grouped = columns.foldLeft(Left(Map.empty): GroupedCols) {
        case (Left(acc), (ColumnRef(path, CArrayType(_)), col)) =>
          val acc0 = acc.map { case (k, v) => (k, Set(v)) }
          Right(acc0 + (path -> Set(col)))

        case (Left(acc), (ColumnRef(path, _), col)) =>
          (acc get path).fold[GroupedCols](Left(acc + (path -> col))) { col0 =>
            val acc0 = acc.map { case (k, v) => (k, Set(v)) }
            Right(acc0 + (path         -> Set(col0, col)))
          }

        case (Right(acc), (ColumnRef(path, _), col)) =>
          Right(acc + (path -> (acc.getOrElse(path, Set.empty[Column]) + col)))
      }

      grouped match {
        case Left(cols0) =>
          val cols = cols0.toList
            .sortBy(_._1)
            .map {
              case (_, col) =>
                Column.rowOrder(col)
            }
            .toArray

          def cmp(i: Int, j: Int): Ordering = {
            var k = 0
            while (k < cols.length) {
              Order(cols(k)).order(i, j) match {
                case EQ  => k += 1
                case cmp => return cmp
              }
            }
            EQ
          }
          scalaz.Order.order(cmp _)

        case Right(cols) =>
          val paths     = cols.keys.toSeq
          val traversal = CPathTraversal(paths.toList)
          traversal.rowOrder(paths.toList, cols)
      }
    }

  def sortWith(keySlice: Slice, sortOrder: DesiredSortOrder): (Slice, Slice) = {

    // We filter out rows that are completely undefined.
    val order: Array[Int] = Array.range(0, source.size) filter { row =>
      keySlice.isDefinedAt(row) && source.isDefinedAt(row)
    }
    val rowOrder = if (sortOrder == SortAscending) keySlice.order else keySlice.order.reverseOrder
    ygg.external.MergeSort.sort(order)(rowOrder, implicitly)

    val remapOrder = new ArrayIntList(order.size)
    var i          = 0
    while (i < order.length) {
      remapOrder.add(i, order(i))
      i += 1
    }

    val sortedSlice    = source.remap(remapOrder)
    val sortedKeySlice = keySlice.remap(remapOrder)

    // TODO Remove the duplicate distinct call. Should be able to handle this in 1 pass.
    (sortedSlice.distinct(None, sortedKeySlice), sortedKeySlice.distinct(None, sortedKeySlice))
  }

  def sortBy(prefixes: Vector[CPath], sortOrder: DesiredSortOrder): Slice = {
    // TODO This is slow... Faster would require a prefix map or something... argh.
    val keySlice = Slice(
      source.size,
      ColumnMap.Eager(prefixes.zipWithIndex.flatMap({
        case (prefix, i) =>
          columns.fields collect {
            case (ColumnRef(path, tpe), col) if path hasPrefix prefix =>
              (ColumnRef(CPathIndex(i) \ path, tpe), col)
          }
      }))
    )

    source sortWith (keySlice, sortOrder = SortAscending) _1
  }

  /**
    * Split the table at the specified index, exclusive. The
    * new prefix will contain all indices less than that index, and
    * the new suffix will contain indices >= that index.
    */
  def split(idx: Int): PairOf[Slice] = take(idx) -> drop(idx)

  def take(sz: Int): Slice = (
    if (sz >= source.size) source
    else Slice(sz, lazyMapColumns(_ |> cf.RemapFilter(_ < sz, 0) get))
  )

  def drop(sz: Int): Slice = (
    if (sz <= 0) source
    else Slice(source.size - sz, lazyMapColumns(_ |> cf.RemapFilter(_ < size, sz) get))
  )

  def takeRange(start: Int, len: Int): Slice = {
    val take = min(size, start + len) - start
    Slice(take, lazyMapColumns(_ |> cf.RemapFilter(_ < take, start) get))
  }

  def zip(other: Slice): Slice = Slice(
    source.size min other.size,
    other.columns.foldLeft(columns) {
      case (acc, (ref, col)) => acc.updated(ref, acc get ref flatMap (c => cf.UnionRight(c, col)) getOrElse col)
    }
  )

  /**
    * This creates a new slice with the same size and columns as this slice, but
    * whose values have been materialized and stored in arrays.
    */
  def materialized: Slice = Slice(
    source.size,
    lazyMapColumns {
      case col: BoolColumn =>
        val defined = col.definedAt(0, source.size)
        val values = Bits.filteredRange(0, source.size) { row =>
          defined(row) && col(row)
        }
        ArrayBoolColumn(defined, values)

      case col: LongColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[Long](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayLongColumn(defined, values)

      case col: DoubleColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[Double](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayDoubleColumn(defined, values)

      case col: NumColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[BigDecimal](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayNumColumn(defined, values)

      case col: StrColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[String](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayStrColumn(defined, values)

      case col: DateColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[DateTime](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayDateColumn(defined, values)

      case col: PeriodColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[Period](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayPeriodColumn(defined, values)

      case col: EmptyArrayColumn =>
        val ncol = MutableEmptyArrayColumn.empty()
        Loop.range(0, source.size) { row =>
          ncol.update(row, col.isDefinedAt(row))
        }
        ncol

      case col: EmptyObjectColumn =>
        val ncol = MutableEmptyObjectColumn.empty()
        Loop.range(0, source.size) { row =>
          ncol.update(row, col.isDefinedAt(row))
        }
        ncol

      case col: NullColumn =>
        val ncol = MutableNullColumn.empty()
        Loop.range(0, source.size) { row =>
          ncol.update(row, col.isDefinedAt(row))
        }
        ncol

      case col =>
        abort("Cannot materialise non-standard (extensible) column")
    }
  )

  def toRValue(row: RowId): RValue = {
    columns.foldLeft[RValue](CUndefined) {
      case (rv, (ColumnRef(selector, _), col)) if col.isDefinedAt(row) => rv.unsafeInsert(selector, col.cValue(row))
      case (rv, _)                                                     => rv
    }
  }

  def toJValue(row: RowId) = {
    columns.foldLeft[JValue](JUndefined) {
      case (jv, (ColumnRef(selector, _), col)) if col.isDefinedAt(row) =>
        cPathToJPaths(selector, col.cValue(row)).foldLeft(jv) {
          case (jv, (path, value)) => jv.unsafeInsert(path, value.toJValue)
        }

      case (jv, _) => jv
    }
  }

  def toJson(row: RowId): JValue = toJValue(row)

  def toJValues: Vector[JValue] = {
    @tailrec def rec(i: Int, acc: Vector[JValue]): Vector[JValue] = {
      if (i < source.size) {
        toJValue(i) match {
          case JUndefined => rec(i + 1, acc)
          case jv         => rec(i + 1, acc :+ jv)
        }
      } else acc
    }

    rec(0, Vector())
  }

  def toString(row: RowId): Option[String] = {
    (columns.toList.sortBy(_._1) map { case (ref, col) => ref.toString + ": " + (if (col.isDefinedAt(row)) col.strValue(row) else "(undefined)") }) match {
      case Nil                                         => None
      case l                                           => Some(l.mkString("[", ", ", "]"))
    }
  }

  def cPathToJPaths(cpath: CPath, value: CValue): Vec[JPath -> CValue] = {
    import ygg.json._

    def add(c: JPathNode, xs: Vec[JPath -> CValue]): Vec[JPath -> CValue] =
      xs map { case (path, value) => (JPath(c :: path.nodes), value) }

    (cpath.nodes, value) match {
      case (Seq(), _)                          => Vec(NoJPath -> value)
      case (CPathField(name) +: tail, _)       => add(JPathField(name), cPathToJPaths(CPath(tail), value))
      case (CPathIndex(i) +: tail, _)          => add(JPathIndex(i), cPathToJPaths(CPath(tail), value))
      case (CPathArray +: tail, es: CArray[_]) =>
        val CArrayType(elemType) = es.cType
        es.value.toVector.zipWithIndex flatMap { case (e, i) => add(JPathIndex(i), cPathToJPaths(CPath(tail), elemType(e))) }
      case (path, _) => abort("Bad news, bob! " + path)
    }
  }

  override def toString = (0 until size).map(toString(_).getOrElse("")).mkString("\n")
}
