
* @tparam S the source
* @tparam T the modified source
* @tparam A the target
* @tparam B the modified target

// "A pair of reversible functions S <=> A and B <=> T
// --> PPrism, PLens
PIso[S, T, A, B] {
  def get(s: S): A
  def reverseGet(b: B): T
  def reverse: PIso[B, A, T, S]
  // reverse.get: T => B
  // reverse.reverseGet: A => S

  def mapping[F[_]: Functor]: PIso[F[S], F[T], F[A], F[B]]
  def modifyF[F[_]: Functor](f: A => F[B])(s: S): F[T]
  def modify(f: A => B): S => T
  def set(b: B): S => T

  def first[C]: PIso[(S, C), (T, C), (A, C), (B, C)]
  def second[C]: PIso[(C, S), (C, T), (C, A), (C, B)]
  def left[C] : PIso[S \/ C, T \/ C, A \/ C, B \/ C]
  def right[C]: PIso[C \/ S, C \/ T, C \/ A, C \/ B]
  def split[S1, T1, A1, B1](other: PIso[S1, T1, A1, B1]): PIso[(S, S1), (T, T1), (A, A1), (B, B1)]
}

// "Weaker PISo where forward function can fail"
// --> Setter, POptional
PPrism[S, T, A, B] {
  def getOrModify(s: S): T \/ A
  def reverseGet(b: B): T
  def getOption(s: S): Option[A]

  def first[C]: PPrism[(S, C), (T, C), (A, C), (B, C)]
  def second[C]: PPrism[(C, S), (C, T), (C, A), (C, B)]
  def left[C] : PPrism[S \/ C, T \/ C, A \/ C, B \/ C]
  def right[C]: PPrism[C \/ S, C \/ T, C \/ A, C \/ B]

  def isMatching(s: S): Boolean
  def modify(f: A => B): S => T
  def modifyF[F[_] : Applicative](f: A => F[B])(s: S): F[T]
  def modifyOption(f: A => B): S => Option[T]
  def re: Getter[B, T]
  def set(b: B): S => T
  def setOption(b: B): S => Option[T]
}

// "Weaker PISo where reverse function requires two parameters"
// --> POptional, Getter
PLens[S, T, A, B] {
  def get(s: S): A         // i.e. from an `S`, we can extract an `A`
  def set(b: B): S => T    // i.e. if we replace an `A` by a `B` in an `S`, we obtain a `T`
  def modifyF[F[_]: Functor](f: A => F[B])(s: S): F[T]
  def modify(f: A => B): S => T

  def first[C]: PLens[(S, C), (T, C), (A, C), (B, C)]
  def second[C]: PLens[(C, S), (C, T), (C, A), (C, B)]

  def choice[S1, T1](other: PLens[S1, T1, A, B]): PLens[S \/ S1, T \/ T1, A, B]
  def split[S1, T1, A1, B1](other: PLens[S1, T1, A1, B1]): PLens[(S, S1), (T, T1), (A, A1), (B, B1)]
}

// "Weaknesses of both PPrism and PLens"
// --> PTraversal
POptional[S, T, A, B] {
  def getOrModify(s: S): T \/ A
  def set(b: B): S => T

  def first[C]: POptional[(S, C), (T, C), (A, C), (B, C)]
  def second[C]: POptional[(C, S), (C, T), (C, A), (C, B)]

  def getOption(s: S): Option[A]
  def modifyF[F[_]: Applicative](f: A => F[B])(s: S): F[T]
  def modify(f: A => B): S => T
  def modifyOption(f: A => B): S => Option[T]
  def setOption(b: B): S => Option[T]
  def isMatching(s: S): Boolean
  def choice[S1, T1](other: POptional[S1, T1, A, B]): POptional[S \/ S1, T \/ T1, A, B]
}

// "POptional generalised to 0+ targets"
// --> Setter, Fold
PTraversal[S, T, A, B] {
  def modifyF[F[_]: Applicative](f: A => F[B])(s: S): F[T]

  def all(p: A => Boolean)(s: S): Boolean
  def choice[S1, T1](other: PTraversal[S1, T1, A, B]): PTraversal[S \/ S1, T \/ T1, A, B]
  def exist(p: A => Boolean)(s: S): Boolean
  def find(p: A => Boolean)(s: S): Option[A]
  def fold(s: S)(implicit ev: Monoid[A]): A
  def foldMap[M: Monoid](f: A => M)(s: S): M
  def getAll(s: S): List[A]
  def headOption(s: S): Option[A]
  def length(s: S): Int
  def modify(f: A => B): S => T
  def set(b: B): S => T
}

// "Generalization of Functor#map: (A => B) => (F[A] => F[B])"
PSetter[S, T, A, B] {
  modify: (A => B) => (S => T)
  set: B => (S => T)
}

// "The degenerate optic is a function S => A"
// --> Fold
Getter[S, A] {
  def get(s: S): A

  def choice[S1](other: Getter[S1, A]): Getter[S \/ S1, A]
  def split[S1, A1](other: Getter[S1, A1]): Getter[(S, S1), (A, A1)]
  def first[B]: Getter[(S, B), (A, B)]
  def second[B]: Getter[(B, S), (B, A)]
  def left[C] : Getter[S \/ C, A \/ C]
  def right[C]: Getter[C \/ S, C \/ A]
}

// "Weaker Traversal which cannot modify the target"
// "Getter generalised to 0+ targets"
Fold[S, A] {
  foldMap[M: Monoid](f: A => M)(s: S): M
}
