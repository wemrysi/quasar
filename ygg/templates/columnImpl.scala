
final class C${{TYPE}}(val value: ${{TYPE}}) extends CWrappedValue[${{TYPE}}]
final object C${{TYPE}} extends CType {
  def apply(value: ${{TYPE}}): C${{TYPE}} = new C${{TYPE}}(value)
}

trait ${{TYPE}}Column extends Column with (${{ROWID}} => ${{TYPE}}) {
  def apply(row: ${{ROWID}}): ${{TYPE}}
  def rowCompare(row1: ${{ROWID}}, row2: ${{ROWID}}): Ordering = Order[${{TYPE}}].order(apply(row1), apply(row2))

  final val tpe: CType                                        = C${{TYPE}}
  final def jValue(row: ${{ROWID}}): JValue                   = JNum(apply(row))
  final def cValue(row: ${{ROWID}}): CWrappedValue[${{TYPE}}] = C${{TYPE}}(apply(row))
  final def strValue(row: ${{ROWID}}): String                 = String.valueOf(apply(row))
  override def toString: String                               = "${{TYPE}}Column"
}

trait Array${{TYPE}}Column extends Column with (${{ROWID}} => Array[${{TYPE}}]) {
  def apply(row: ${{ROWID}}): Array[${{TYPE}}]
  def rowCompare(row1: ${{ROWID}}, row2: ${{ROWID}}): Ordering = Order[Vector[${{TYPE}}]].order(apply(row1).toVector, apply(row2).toVector)

  final val tpe: CArrayType[${{TYPE}}]                               = CArrayType[${{TYPE}}](C${{TYPE}})
  final def jValue(row: ${{ROWID}}): JValue                          = jarray(apply(row) map (n => JNum(n)): _*)
  final def cValue(row: ${{ROWID}}): CWrappedValue[Array[${{TYPE}}]] = CArray(apply(row), tpe)
  final def strValue(row: ${{ROWID}}): String                        = apply(row) mkString ("[", ", ", "]")
  override def toString: String                                      = "Array${{TYPE}}Column"
}
