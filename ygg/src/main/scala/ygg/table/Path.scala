package ygg.table

import ygg.common._

final case class Path private (components: Vector[String]) {
  def isEmpty                 = components.isEmpty
  def path: String            = (components mkString "/").replaceAll("/+", "/")
  def length: Int             = components.length
  def parent: Path            = if (isEmpty) this else Path(components.init)
  def ancestors: Vector[Path] = if (isEmpty) Vec() else parent +: parent.ancestors

  def isEqualOrParentOf(that: Path) = that.components startsWith this.components

  def mapSegments(f: Vec[String] => Vec[String]): Path = Path(f(components))

  def /(that: Path)                     = mapSegments(_ ++ that.components)
  def rollups(depth: Int): Vector[Path] = this +: (ancestors take depth)
  def urlEncode: Path                   = mapSegments(_ map (c => java.net.URLEncoder.encode(c, "UTF-8")))
  def prefix: Path                      = if (isEmpty) this else mapSegments(_.init)
  override def toString                 = path
}

object Path {
  val Root = new Path(Vec())

  private def cleanPath(string: String): String = string.replaceAll("^/|/$", "").replaceAll("/+", "/")

  def apply(path: String): Path       = new Path(cleanPath(path).split("/").filterNot(_.trim.isEmpty).toVector)
  def apply(elems: Seq[String]): Path = apply(elems mkString "/")
}
