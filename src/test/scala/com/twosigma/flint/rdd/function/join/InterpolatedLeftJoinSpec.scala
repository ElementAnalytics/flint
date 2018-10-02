package com.twosigma.flint.rdd.function.join

import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD }
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class InterpolatedJoinSpec extends FlatSpec with SharedSparkContext {

  val left = Array(
    (1000f, ("a", 1)),
    (1010f, ("b", 5)),
    (1020f, ("c", 11)),
    (1030f, ("e", 15))
  )

  val right = Array(
    (1004f, ("g", 31)),
    (1008f, ("h", 41)),
    (1012f, ("v", 71)),
    (1016f, ("j", 81)),
    (1020f, ("k", 111))
  )

  var leftRdd: OrderedRDD[Float, (String, Int)] = _
  var rightRdd: OrderedRDD[Float, (String, Int)] = _

  override def beforeAll() {
    super.beforeAll()
    leftRdd = OrderedRDD.fromRDD(sc.parallelize(left, 4), KeyPartitioningType.Sorted)
    rightRdd = OrderedRDD.fromRDD(sc.parallelize(right, 4), KeyPartitioningType.Sorted)
  }

  it should "`interpolate` when given two OrderedRDDs" in {
    val expected = Array(
      (1000f, (("a", 1), None)),
      (1010f, (("b", 5), Some(("v", 56)))),
      (1020f, (("c", 11), Some(("k", 111)))),
      (1030f, (("e", 15), None))
    )

    val epsilon = 1e-4f

    implicit val floatEq = TolerantNumerics.tolerantFloatEquality(epsilon)
    implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

    val skFn = {
      case ((sk, v)) => None
    }: ((String, Int)) => Option[Nothing]
    val pastFn: Float => Float = (t: Float) => t - 5
    val futureFn: Float => Float = (t: Float) => t + 5
    val averagingFn: (Float, (Float, (String, Int)), (Float, ((String, Int)))) => (String, Int) = {
      (key: Float, past: (Float, (String, Int)), future: (Float, (String, Int))) =>
        (past, future) match {
          case ((t1, _), (t2, v2)) if (t1 == t2) => v2
          case ((t1, (s1, d1)), (t2, (s2, d2))) => {
            val frac = (key - t1) / (t2 - t1)
            val dAve = (frac * d2 + (1 - frac) * d1).toInt
            val sAve = if (frac >= 0.5) s2 else s1
            (sAve, dAve)
          }
        }
    }
    val joined = InterpolatedLeftJoin.apply(
      leftRdd, rightRdd,
      pastFn, futureFn,
      skFn, skFn,
      averagingFn
    ).collect()
    assert(joined.deep == expected.deep)
  }
}