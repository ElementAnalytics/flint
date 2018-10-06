package com.twosigma.flint.rdd.function.join

import java.util
import java.util.{ HashMap => JHashMap }

import com.twosigma.flint.rdd.{ OrderedRDD, PartitionsIterator, PeekableIterator }
import org.apache.spark.{ NarrowDependency, OneToOneDependency }

import scala.collection.immutable.TreeMap
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

protected[flint] object InterpolatedLeftJoin {

  val skMapInitialSize = 1024

  def apply[K: ClassTag, SK, V, V2](
    leftRdd: OrderedRDD[K, V],
    rightRdd: OrderedRDD[K, V2],
    pastToleranceFn: K => K,
    futureToleranceFn: K => K,
    leftSk: V => SK,
    rightSk: V2 => SK,
    interpolationFn: (K, (K, V2), (K, V2)) => V2,
    strictForward: Boolean = false
  )(implicit ord: Ordering[K]): OrderedRDD[K, (V, Option[V2])] = {

    val leftRangeSplits = leftRdd.rangeSplits
    val rightRangeSplits = rightRdd.rangeSplits

    val indexToWindowJoinSplits = TreeMap(RangeMergeJoin
      .windowJoinSplits(
        (k: K) => (pastToleranceFn(k), futureToleranceFn(k)),
        leftRangeSplits,
        rightRangeSplits
      )
      .map { case (split, parts) => (split.partition.index, (split, parts)) }: _*)

    val leftDep = new OneToOneDependency(leftRdd)

    val rightWindowDep = new NarrowDependency(rightRdd) {
      override def getParents(partitionId: Int) =
        indexToWindowJoinSplits(partitionId)._2.map(_.index)
    }

    val rightWindowPartitions =
      leftRdd.sc.broadcast(indexToWindowJoinSplits.map {
        case (idx, joinSplit) => (idx, joinSplit._2)
      })

    val joinedWindowSplits = indexToWindowJoinSplits.map {
      case (_, (split, _)) => split
    }.toArray

    new OrderedRDD[K, (V, Option[V2])](
      leftRdd.sc,
      joinedWindowSplits,
      Seq(leftDep, rightWindowDep)
    )(
      (part, context) => {
        val parts = rightWindowPartitions.value(part.index)
        val rightPastIter =
          PeekableIterator(PartitionsIterator(rightRdd, parts, context))
        val rightFutureIter =
          PeekableIterator(PartitionsIterator(rightRdd, parts, context))
        val lastSeen = new JHashMap[SK, (K, V2)](skMapInitialSize)
        val foreSeen = new JHashMap[SK, util.Deque[(K, V2)]]()
        leftRdd.iterator(part, context).map {
          case (k, v) =>
            val sk = leftSk(v)
            LeftJoin.catchUp(k, rightSk, rightPastIter, lastSeen)

            val past = Option(lastSeen.get(sk)).filter { t =>
              ord.gteq(t._1, pastToleranceFn(k))
            }

            val queueForFuture = FutureLeftJoin.forward(
              k,
              sk,
              futureToleranceFn(k),
              rightSk,
              rightFutureIter,
              foreSeen,
              strictForward
            )
            val future =
              if (queueForFuture.isEmpty) None
              else Some(queueForFuture.peekFirst())

            val interpolated = (past, future) match {
              case (None, None) => None
              case (None, Some((_, value))) => None
              case (Some((_, value)), None) => Some(value)
              case (Some((kPast, vPast)), Some((kFuture, vFuture))) => {
                Some(interpolationFn(k, (kPast, vPast), (kFuture, vFuture)))
              }
            }
            (k, (v, interpolated))
        }
      }
    )
  }
}
