package ygg.data

import ygg.common._
import scalaz.Either3, Either3._

final case class Cogrouped[K, V, V1, CC[X] <: Traversable[X]](lmap: scMap[K, CC[V]], rmap: scMap[K, CC[V1]]) {
  lazy val leftKeys   = lmap.keySet -- rmap.keySet
  lazy val middleKeys = lmap.keySet & rmap.keySet
  lazy val rightKeys  = rmap.keySet -- lmap.keySet

  def build[That](implicit cbf: CBF[_, K -> Either3[V, CC[V] -> CC[V1], V1], That]): That = {
    val buf = cbf()

    for (k <- leftKeys ; v <- lmap(k)) buf += (k -> left3(v))
    for (k <- rightKeys ; v <- rmap(k)) buf += (k -> right3(v))
    for (k <- middleKeys) buf += (k -> middle3(lmap(k) -> rmap(k)))

    buf.result()
  }
}
