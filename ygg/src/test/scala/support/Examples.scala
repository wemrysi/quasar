package ygg.tests

import blueeyes._, json._

object Examples {
  val quoted  = """["foo \" \n \t \r bar"]"""
  val symbols = JObject(JField("f1", JString("foo")) :: JField("f2", JString("bar")) :: Nil)

  def lotto = """
{
  "lotto":{
    "lotto-id":5,
    "winning-numbers":[2,45,34,23,7,5,3],
    "winners":[ {
      "winner-id":23,
      "numbers":[2,45,34,23,3, 5]
    },{
      "winner-id" : 54 ,
      "numbers":[ 52,3, 12,11,18,22 ]
    }]
  }
}
"""

  def person = """
{
  "person": {
    "name": "Joe",
    "age": 35.0,
    "spouse": {
      "person": {
        "name": "Marilyn",
        "age": 33.0
      }
    }
  }
}
"""

  def personDSL =
    JObject(
      JField(
        "person",
        JObject(
          JField("name", JString("Joe")) ::
            JField("age", JNum(35)) ::
              JField(
                "spouse",
                JObject(
                  JField(
                    "person",
                    JObject(
                      JField("name", JString("Marilyn")) ::
                        JField("age", JNum(33)) :: Nil
                    )) :: Nil
                )) :: Nil
        )) :: Nil
    )

  val objArray = """
{ "name": "joe",
  "address": {
    "street": "Bulevard",
    "city": "Helsinki"
  },
  "children": [
    {
      "name": "Mary",
      "age": 5.0
    },
    {
      "name": "Mazy",
      "age": 3.0
    }
  ]
}
"""
}
