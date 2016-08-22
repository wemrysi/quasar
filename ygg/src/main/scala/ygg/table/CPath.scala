package ygg.table

import ygg.common._
import scalaz._, Scalaz._, Ordering._
import ygg.json._

sealed trait CPath {
  def nodes: List[CPathNode]
}
private[table] case class CPathClass(nodes: List[CPathNode]) extends CPath {
  override def toString: String = if (nodes.isEmpty) "." else nodes mkString ""
}

object CPath {
  private val PathPattern  = """\.|(?=\[\d+\])|(?=\[\*\])""".r
  private val IndexPattern = """^\[(\d+)\]$""".r

  val Identity = CPath()

  type AndValue = CPath -> CValue

  def apply(l: List[CPathNode]): CPath = CPathClass(l)
  def apply(n: CPathNode*): CPath      = apply(n.toList)
  def apply(path: JPath): CPath        = apply(
    path.nodes map {
      case JPathField(name) => CPathField(name)
      case JPathIndex(idx)  => CPathIndex(idx)
    }
  )

  def unapplySeq(path: CPath): Option[List[CPathNode]]  = Some(path.nodes)
  def unapplySeq(path: String): Option[List[CPathNode]] = Some(apply(path).nodes)

  implicit def apply(path: String): CPath = {
    def parse0(segments: List[String], acc: List[CPathNode]): List[CPathNode] = segments match {
      case Nil                               => acc
      case head :: tail if head.trim.isEmpty => parse0(tail, acc)
      case "[*]" :: tail                     => parse0(tail, CPathArray :: acc)
      case IndexPattern(index) :: tail       => parse0(tail, CPathIndex(index.toInt) :: acc)
      case name :: tail                      => parse0(tail, CPathField(name) :: acc)
    }

    val properPath = if (path.startsWith(".")) path else "." + path
    apply(parse0(PathPattern.split(properPath).toList, Nil).reverse: _*)
  }

  trait CPathTree[A]
  case class RootNode[A](children: Seq[CPathTree[A]])                     extends CPathTree[A]
  case class FieldNode[A](field: CPathField, children: Seq[CPathTree[A]]) extends CPathTree[A]
  case class IndexNode[A](index: CPathIndex, children: Seq[CPathTree[A]]) extends CPathTree[A]
  case class LeafNode[A](value: A)                                        extends CPathTree[A]

  case class PathWithLeaf[A](path: Seq[CPathNode], value: A) {
    val size: Int = path.length
  }

  def makeStructuredTree[A](pathsAndValues: Seq[CPath -> A]) = {
    def inner[A](paths: Seq[PathWithLeaf[A]]): Seq[CPathTree[A]] = {
      if (paths.size == 1 && paths.head.size == 0) {
        List(LeafNode(paths.head.value))
      } else {
        val filtered = paths filterNot { case PathWithLeaf(path, _)  => path.isEmpty }
        val grouped  = filtered groupBy { case PathWithLeaf(path, _) => path.head }

        def recurse[A](paths: Seq[PathWithLeaf[A]]) =
          inner(paths map { case PathWithLeaf(path, v) => PathWithLeaf(path.tail, v) })

        val result = grouped.toSeq.sortBy(_._1) map {
          case (node, paths) =>
            node match {
              case (field: CPathField) => FieldNode(field, recurse(paths))
              case (index: CPathIndex) => IndexNode(index, recurse(paths))
              case _                   => abort("CPathArray and CPathMeta not supported")
            }
        }
        result
      }
    }

    val leaves = pathsAndValues.sortBy(_._1) map {
      case (path, value) =>
        PathWithLeaf[A](path.nodes, value)
    }

    RootNode(inner(leaves))
  }

  def makeTree[A](cpaths0: Seq[CPath], values: Seq[A]): CPathTree[A] = {
    if (cpaths0.isEmpty && values.length == 1)
      RootNode(Seq(LeafNode(values.head)))
    else if (cpaths0.length == values.length)
      makeStructuredTree(cpaths0.sorted zip values)
    else
      RootNode(Seq.empty[CPathTree[A]])
  }

  implicit def singleNodePath(node: CPathNode): CPath = CPath(node)

  implicit val CPathOrder: Ord[CPath] = Ord order { (v1, v2) =>
    def compare0(n1: List[CPathNode], n2: List[CPathNode]): Cmp = (n1, n2) match {
      case (Nil, Nil) => EQ
      case (Nil, _)   => LT
      case (_, Nil)   => GT
      case (n1 :: ns1, n2 :: ns2) =>
        val ncomp = Ord[CPathNode].order(n1, n2)
        if (ncomp != EQ) ncomp else compare0(ns1, ns2)
    }

    compare0(v1.nodes, v2.nodes)
  }

  implicit class CPathOps(private val self: CPath) extends AnyVal {
    import self.nodes

    def combine(paths: Seq[CPath]): Seq[CPath] = (
      if (paths.isEmpty) Seq(self)
      else paths map (p => CPath(nodes ++ p.nodes))
    )

    def \(that: CPath): CPath  = CPath(nodes ++ that.nodes)
    def \(that: String): CPath = CPath(nodes :+ CPathField(that))
    def \(that: Int): CPath    = CPath(nodes :+ CPathIndex(that))

    def hasPrefix(p: CPath): Boolean = nodes startsWith p.nodes
  }
}
