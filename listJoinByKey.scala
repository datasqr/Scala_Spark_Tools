def listJoinByKey[T](l1: Traversable[(T,T)], l2: Traversable[(T,T)]): Traversable[(T,(T,T))] ={

    l1.map(tup1 => l2.filter(tup2 => tup1._1==tup2._1).map(tup2 => (tup1._1, (tup1._2, tup2._2)))).flatten

}
