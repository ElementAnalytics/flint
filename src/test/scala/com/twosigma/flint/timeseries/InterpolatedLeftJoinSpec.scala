package com.twosigma.flint.timeseries

import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD }
import com.twosigma.flint.timeseries.clock.UniformClock
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.{ SharedSparkContext }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{ GenericRowWithSchema => ExternalRow }
import org.apache.spark.sql.types._
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class InterpolatedLeftJoinSpec extends FlatSpec with SharedSparkContext {

  private val defaultPartitionParallelism: Int = 5

  val epsilon = 1e-4f

  implicit val floatEq = TolerantNumerics.tolerantFloatEquality(epsilon)
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  "Interpolated Join" should "Correctly interpolate" in {

    val leftSchema = Schema("time" -> LongType, "str2" -> StringType, "int2" -> IntegerType, "float2" -> FloatType)
    val resultSchema = Schema("str2" -> StringType, "int2" -> IntegerType, "float2" -> FloatType)

    val left = Array[(Long, Row)](
      (1004L, new ExternalRow(Array(1004L, "g", 31, 0.31f), leftSchema)),
      (1008L, new ExternalRow(Array(1008L, "h", 41, 0.41f), leftSchema)),
      (1012L, new ExternalRow(Array(1012L, "v", 71, 0.71f), leftSchema)),
      (1016L, new ExternalRow(Array(1016L, "j", 81, null), leftSchema)),
      (1020L, new ExternalRow(Array(1020L, null, 111, 1.11f), leftSchema))
    )

    val expected = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, null, null, null), resultSchema)),
      (1005L, new ExternalRow(Array(1005L, "g", 31, 0.335f), resultSchema)),
      (1010L, new ExternalRow(Array(1010L, "h", 41, 0.56f), resultSchema)),
      (1015L, new ExternalRow(Array(1015L, "v", 71, null), resultSchema)),
      (1020L, new ExternalRow(Array(1020L, null, 111, 1.11f), resultSchema)),
      (1025L, new ExternalRow(Array(1025L, null, 111, 1.11f), resultSchema))
    )
    val leftRdd = OrderedRDD.fromRDD(sc.parallelize(left, 4), KeyPartitioningType.Sorted)

    val leftTSRdd = TimeSeriesRDD.fromOrderedRDD(leftRdd, leftSchema)

    val resultsRdd = leftTSRdd.interpolate(1000L, 1040L, 5L, 5L, 5L)

    assert(resultsRdd.schema === leftSchema)
    assert(resultsRdd.collect().deep === expected.map(_._2).deep)
  }

  "Interpolated Join" should "Correctly interpolate using key" in {

    val leftSchema = Schema("time" -> LongType, "key" -> StringType, "str2" -> StringType,
      "int2" -> IntegerType, "float2" -> FloatType)
    val resultSchema = Schema("key" -> StringType, "str2" -> StringType, "int2" -> IntegerType, "float2" -> FloatType)

    val left = Array[(Long, Row)](
      (1004L, new ExternalRow(Array(1004L, "a", "g", 31, 0.31f), leftSchema)),
      (1008L, new ExternalRow(Array(1008L, "a", "h", 41, 0.41f), leftSchema)),
      (1012L, new ExternalRow(Array(1012L, "a", "v", 71, 0.71f), leftSchema)),
      (1016L, new ExternalRow(Array(1016L, "a", "j", 81, null), leftSchema)),
      (1020L, new ExternalRow(Array(1020L, "a", null, 111, 1.11f), leftSchema)),

      (1004L, new ExternalRow(Array(1004L, "b", "g", 31, 0.31f), leftSchema)),
      (1008L, new ExternalRow(Array(1008L, "b", "h", 41, 0.41f), leftSchema)),
      (1012L, new ExternalRow(Array(1012L, "b", "v", 71, 0.71f), leftSchema)),
      (1016L, new ExternalRow(Array(1016L, "b", "j", 81, null), leftSchema)),
      (1020L, new ExternalRow(Array(1020L, "b", null, 111, 1.11f), leftSchema))
    )

    val expected = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, "a", null, null, null), resultSchema)),
      (1000L, new ExternalRow(Array(1000L, "b", null, null, null), resultSchema)),
      (1005L, new ExternalRow(Array(1005L, "a", "g", 31, 0.335f), resultSchema)),
      (1005L, new ExternalRow(Array(1005L, "b", "g", 31, 0.335f), resultSchema)),
      (1010L, new ExternalRow(Array(1010L, "a", "h", 41, 0.56f), resultSchema)),
      (1010L, new ExternalRow(Array(1010L, "b", "h", 41, 0.56f), resultSchema)),
      (1015L, new ExternalRow(Array(1015L, "a", "v", 71, null), resultSchema)),
      (1015L, new ExternalRow(Array(1015L, "b", "v", 71, null), resultSchema)),
      (1020L, new ExternalRow(Array(1020L, "a", null, 111, 1.11f), resultSchema)),
      (1020L, new ExternalRow(Array(1020L, "b", null, 111, 1.11f), resultSchema)),
      (1025L, new ExternalRow(Array(1025L, "a", null, 111, 1.11f), resultSchema)),
      (1025L, new ExternalRow(Array(1025L, "b", null, 111, 1.11f), resultSchema))
    )

    val expectedMap = expected.map(t => ((t._1, t._2.get(1)) -> t._2)).toMap

    val leftRdd = OrderedRDD.fromRDD(sc.parallelize(left, 4), KeyPartitioningType.UnSorted)

    val leftTSRdd = TimeSeriesRDD.fromOrderedRDD(leftRdd, leftSchema)

    val resultsRdd = leftTSRdd.interpolate(1000L, 1040L, 5L, 5L, 5L, Seq("key"))

    val resultMap = resultsRdd.collect().map(row => (row.getLong(0), row.get(1)) -> row).toMap

    assert(resultsRdd.schema === leftSchema)
    resultMap.foreach {
      case (k, v) => {
        assert(expectedMap(k) == v)
      }
    }
  }

  "Interpolated Join" should "Correctly interpolate using a lookback function" in {

    val leftSchema = Schema("time" -> LongType, "str2" -> StringType, "int2" -> IntegerType, "float2" -> FloatType)
    val resultSchema = Schema("str2" -> StringType, "int2" -> IntegerType, "float2" -> FloatType)

    val left = Array[(Long, Row)](
      (1004L, new ExternalRow(Array(1004L, "g", 31, 0.31f), leftSchema)),
      (1008L, new ExternalRow(Array(1008L, "h", 41, 0.41f), leftSchema)),
      (1012L, new ExternalRow(Array(1012L, "v", 71, 0.71f), leftSchema)),
      (1016L, new ExternalRow(Array(1016L, "j", 81, null), leftSchema)),
      (1020L, new ExternalRow(Array(1020L, null, 111, 1.11f), leftSchema))
    )

    val expected = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, null, null, null), resultSchema)),
      (1005L, new ExternalRow(Array(1005L, "g", 31, 0.31f), resultSchema)),
      (1010L, new ExternalRow(Array(1010L, "h", 41, 0.41f), resultSchema)),
      (1015L, new ExternalRow(Array(1015L, "v", 71, 0.71f), resultSchema)),
      (1020L, new ExternalRow(Array(1020L, null, 111, 1.11f), resultSchema)),
      (1025L, new ExternalRow(Array(1025L, null, 111, 1.11f), resultSchema))
    )
    val leftRdd = OrderedRDD.fromRDD(sc.parallelize(left, 4), KeyPartitioningType.Sorted)

    val leftTSRdd = TimeSeriesRDD.fromOrderedRDD(leftRdd, leftSchema)

    val resultsRdd = leftTSRdd.interpolate(1000L, 1040L, 5L, 5L, 5L, interpolation = "lookback")

    assert(resultsRdd.schema === leftSchema)
    assert(resultsRdd.collect().deep === expected.map(_._2).deep)
  }

  "Interpolated Join" should "Correctly join two RDDs" in {

    val leftSchema = Schema("time" -> LongType, "str1" -> StringType, "int1" -> IntegerType)
    val rightSchema = Schema("time" -> LongType, "str2" -> StringType, "int2" -> IntegerType, "float2" -> FloatType)
    val resultSchema = Schema("str1" -> StringType, "int1" -> IntegerType, "str2" -> StringType,
      "int2" -> IntegerType, "float2" -> FloatType)

    val left = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, "a", 1), leftSchema)),
      (1010L, new ExternalRow(Array(1010L, "b", 5), leftSchema)),
      (1020L, new ExternalRow(Array(1020L, "c", 11), leftSchema)),
      (1030L, new ExternalRow(Array(1030L, "e", 15), leftSchema))
    )

    val right = Array[(Long, Row)](
      (1004L, new ExternalRow(Array(1004L, "g", 31, 0.31f), rightSchema)),
      (1008L, new ExternalRow(Array(1008L, "h", 41, 0.41f), rightSchema)),
      (1012L, new ExternalRow(Array(1012L, "v", 71, 0.71f), rightSchema)),
      (1016L, new ExternalRow(Array(1016L, "j", 81, 0.81f), rightSchema)),
      (1020L, new ExternalRow(Array(1020L, "k", 111, 1.11f), rightSchema))
    )

    val expected = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, "a", 1, null, null, null), resultSchema)),
      (1010L, new ExternalRow(Array(1010L, "b", 5, "h", 41, 0.56f), resultSchema)),
      (1020L, new ExternalRow(Array(1020L, "c", 11, "k", 111, 1.11f), resultSchema)),
      (1030L, new ExternalRow(Array(1030L, "e", 15, null, null, null), resultSchema))
    )
    val leftRdd = OrderedRDD.fromRDD(sc.parallelize(left, 4), KeyPartitioningType.Sorted)
    val rightRdd = OrderedRDD.fromRDD(sc.parallelize(right, 4), KeyPartitioningType.Sorted)

    val leftTSRdd = TimeSeriesRDD.fromOrderedRDD(leftRdd, leftSchema)
    val rightTSRdd = TimeSeriesRDD.fromOrderedRDD(rightRdd, rightSchema)

    val resultsRdd = leftTSRdd.interpolatedLeftJoin(rightTSRdd, 5L, 5L)

    assert(resultsRdd.schema === resultSchema)
    assert(resultsRdd.collect().deep === expected.map(_._2).deep)
  }

}