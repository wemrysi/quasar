package ygg.json

/** Function to merge two JSONs.
  */
object Merge {

  /** Return merged JSON.
    */
  def merge(val1: JValue, val2: JValue): JValue = (val1, val2) match {
    case (JObject(xs), JObject(ys)) => JObject(mergeFields(xs, ys))
    case (JArray(xs), JArray(ys))   => JArray(mergeVals(xs, ys))
    case (JUndefined, x)            => x
    case (x, JUndefined)            => x
    case (_, y)                     => y
  }

  private[json] def mergeFields(vs1: Map[String, JValue], vs2: Map[String, JValue]): Map[String, JValue] = {
    def mergeRec(xleft: Map[String, JValue], yleft: Map[String, JValue]): Map[String, JValue] = {
      if (xleft.isEmpty) yleft
      else {
        val (xn, xv) = xleft.head
        val xs       = xleft.tail

        yleft.get(xn) match {
          case Some(yv) => mergeRec(xs, yleft - xn) + ((xn, merge(xv, yv)))
          case None     => mergeRec(xs, yleft) + ((xn, xv))
        }
      }
    }

    mergeRec(vs1, vs2)
  }

  private[json] def mergeVals(vs1: Vector[JValue], vs2: Vector[JValue]): Vector[JValue] = {
    def mergeRec(xleft: Vector[JValue], yleft: Vector[JValue]): Vector[JValue] = xleft match {
      case Seq()   => yleft
      case x +: xs =>
        yleft find (_ == x) match {
          case Some(y) => merge(x, y) +: mergeRec(xs, yleft.filterNot(_ == y))
          case None    => x +: mergeRec(xs, yleft)
        }
    }

    mergeRec(vs1, vs2)
  }
}
