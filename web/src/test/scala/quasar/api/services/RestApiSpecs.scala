package quasar
package api
package services

import org.specs2.mutable.Specification

class RestApiSpecs extends Specification {
  "OPTIONS" should {
    "advertise GET and POST for /query path" >> todo
    "advertise Destination header for /query path and method POST" >> todo
    "advertise GET, PUT, POST, DELETE, and MOVE for /data path" >> todo
    "advertise Destination header for /data path and method MOVE" >> todo
  }
}
