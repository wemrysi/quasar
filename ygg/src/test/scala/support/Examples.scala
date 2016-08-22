package ygg.tests

import ygg.json._

object Examples {
  val symbols = json"""{"f1":"foo","f2":"bar"}"""

  def lotto = json"""
    {"lotto":{"lotto-id":5,"winners":[{"numbers":[2,45,34,23,3,5],"winner-id":23},
    {"numbers":[52,3,12,11,18,22],"winner-id":54}],"winning-numbers":[2,45,34,23,7,5,3]}}
  """

  def person = json"""
    {"person":{"age":35.0,"name":"Joe","spouse":{"person":{"age":33.0,"name":"Marilyn"}}}}
  """

  def personDSL = json"""
    {"person":{"age":35,"name":"Joe","spouse":{"person":{"age":33,"name":"Marilyn"}}}}
  """

  def objArray = json"""
    {"address":{"city":"Helsinki","street":"Bulevard"},"children":[{"age":5.0,"name":"Mary"},{"age":3.0,"name":"Mazy"}],"name":"joe"}
  """
}
