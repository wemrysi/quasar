package ygg.api

import ygg.table._

trait FNModule {
  type F1
  type F2

  implicit def liftF1(f1: F1): F1Like
  implicit def liftF2(f2: F2): F2Like

  trait F1Like {
    def compose(f1: F1): F1
    def andThen(f1: F1): F1
  }
  trait F2Like {
    def applyl(cv: CValue): F1
    def applyr(cv: CValue): F1
    def andThen(f1: F1): F2
  }
}

trait FNDummyModule extends FNModule {
  type F1 = ygg.table.CF1
  type F2 = ygg.table.CF2
  implicit def liftF1(f1: F1): F1Like = ???
  implicit def liftF2(f2: F2): F2Like = ???
}
