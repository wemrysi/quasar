package quasar.precog

trait ToString {
  def to_s: String
  final override def toString(): String = to_s
}
