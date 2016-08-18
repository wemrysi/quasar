package blueeyes

import scala.collection.generic.CanBuildFrom
import scalaz._

class ScalaMapOps[A, B, CC[B] <: Traversable[B]](left: scMap[A, CC[B]]) {
  def cogroup[C, CC2[C] <: Traversable[C], Result](right: scMap[A, CC2[C]])(implicit cbf: CanBuildFrom[Nothing, (A, Either3[B, (CC[B], CC2[C]), C]), Result],
                                                                            cbfLeft: CanBuildFrom[CC[B], B, CC[B]],
                                                                            cbfRight: CanBuildFrom[CC2[C], C, CC2[C]]): Result = {
    val resultBuilder = cbf()

    left foreach {
      case (key, leftValues) => {
        right get key map { rightValues =>
          resultBuilder += (key -> Either3.middle3[B, (CC[B], CC2[C]), C]((leftValues, rightValues)))
        } getOrElse {
          leftValues foreach { b =>
            resultBuilder += (key -> Either3.left3[B, (CC[B], CC2[C]), C](b))
          }
        }
      }
    }

    right foreach {
      case (key, rightValues) => {
        if (!(left get key isDefined)) {
          rightValues foreach { c =>
            resultBuilder += (key -> Either3.right3[B, (CC[B], CC2[C]), C](c))
          }
        }
      }
    }

    resultBuilder.result()
  }
}
