/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.online

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.types._

import scala.annotation.switch

case class RandomSeed() extends LeafExpression {
  type EvaluatedType = Any

  def dataType: DataType = IntegerType
  def nullable: Boolean = false
  override def foldable: Boolean = false
  override def toString = "RandomSeed()"

  private[this] var x = 123456789L

  override def eval(input: Row): Any = {
    x ^= (x << 21)
    x ^= (x >>> 35)
    x ^= (x << 4)

    var rand = (x & 0xFFFFFFFF).toInt
    if (rand < 0) {
      rand = -rand
    }
    if (rand >= 1073741823) {
      rand >>= 3
    }
    (rand << 2) | 1
  }
}

// !Hack: In general, we represent the multiplicity vector as an array of long.
// But in order to pack them tight together, we should note that the number of bits
// needs to represent a single multiplicity may grow. E.g., at the input, each multiplicity
// will probably fall within 0-4,
// but after a two-way join, the range may grow to 0-16, so on so forth.
case class Poisson() extends LeafExpression {
  type EvaluatedType = Any

  def dataType: DataType = ByteType
  def nullable: Boolean = false
  override def foldable: Boolean = false
  override def toString = "Poisson()"

  var current: Int = 5

//  private[this] val cdf = {
//    val table = new Array[Double](16)
//    var pmf = 1 / math.E
//
//    table(0) = pmf
//    var i = 1
//    while (i < 15) {
//      pmf /= i
//      table(i) = table(i-1) + pmf
//      i += 1
//    }
//    table(15) = 1.0D
//
//    val norm = new Array[Long](16)
//    i = 0
//    while (i < 16) {
//      norm(i) = (table(i) * 4294967296L).toLong
//      i += 1
//    }
//    norm
//  }
//  [-567453481, 1012576688, 1802591772, 2065930134,
//    2131764724, 2144931642, 2147126128, 2147439627,
//    2147478814, 2147483168, 2147483603, 2147483643,
//    2147483646, 2147483646, 2147483646, 2147483647]

  override def eval(input: Row): Any = {
    current *= 663608941

    // Hacky: but safe with 12 nines
    if (current <= -567453481) 0.toByte
    else if (current <= 1012576688) 1.toByte
    else if (current <= 1802591772) 2.toByte
    else if (current <= 2065930134) 3.toByte
    else if (current <= 2131764724) 4.toByte
    else if (current <= 2144931642) 5.toByte
    else if (current <= 2147126128) 6.toByte
    else if (current <= 2147439627) 7.toByte
    else if (current <= 2147478814) 8.toByte
    else if (current <= 2147483168) 9.toByte
    else if (current <= 2147483603) 10.toByte
    else if (current <= 2147483643) 11.toByte
    else if (current == 2147483647) 15.toByte
    else 12.toByte
  }
}

case class SetSeedAndPoisson(seed: Expression, poisson: Poisson) extends Expression {
  type EvaluatedType = Any

  def children = seed :: poisson :: Nil

  def dataType: DataType = ByteType
  def nullable: Boolean = false
  override def foldable: Boolean = false

  override def eval(input: Row): Any = {
    poisson.current = seed.eval(input).asInstanceOf[Int]
    // poisson.eval(input)
    1.toByte
  }

  override def toString = s"Poisson($seed)"
}

case class ByteMultiply(left: Expression, right: Expression) extends BinaryExpression {
  self: Product =>

  type EvaluatedType = Any

  override def symbol: String = "*"

  override def nullable: Boolean = left.nullable || right.nullable

  override lazy val resolved =
    left.resolved && right.resolved &&
      left.dataType == ByteType &&
      right.dataType == ByteType

  def dataType: DataType = ByteType

  override def eval(input: Row): Any = {
    val evalE2: Int = right.eval(input).asInstanceOf[Byte] & 0xFF
    (evalE2: @switch) match {
      case 0 => 0.toByte
      case 1 => left.eval(input)
      case n => ((left.eval(input).asInstanceOf[Byte] & 0xFF) * n).toByte
    }
  }
}

case class Delegate(multiplicity: Attribute, aggregate: AggregateExpression)
  extends AggregateExpression {
  override type EvaluatedType = Any

  override def nullable: Boolean = aggregate.nullable
  override def dataType: DataType = aggregate.dataType

  override def children: Seq[Expression] = multiplicity :: aggregate :: Nil

  override def toString = s"Delegate($multiplicity, $aggregate)"

  override def newInstance(): AggregateFunction =
    throw new TreeNodeException(this,
      s"No function to instantiate this aggregate. type: ${this.nodeName}")
}

case class Count01(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {
  require(child.dataType == BooleanType && !child.nullable,
    s"!!! Warning: $child has unexpected child type ${child.dataType}")

  override def nullable = false

  override def dataType: DataType = ByteType

  override def toString = s"COUNT01($child)"

  override def newInstance(): AggregateFunction = new Count01Function(child, this)
}

case class Count01Function(expr: Expression, base: AggregateExpression)
  extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  private[this] var seen: Boolean = false

  override def update(input: Row): Unit = {
    if (!seen && expr.eval(input).asInstanceOf[Boolean]) {
      seen = true
    }
  }

  override def eval(input: Row): Any = if (seen) 1.toByte else 0.toByte
}

case class ByteNonZero(child: Expression) extends UnaryExpression with Predicate {
  require(child.dataType == ByteType, s"Incompatible type: ${child.dataType}")

  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"($child != 0)"

  override def eval(input: Row): Any = child.eval(input).asInstanceOf[Byte] != 0.toByte
}

abstract class DelegateAggregateExpression extends AggregateExpression {
  self: Product =>

  val delegatee: Delegatee
  val offset: Int

  override def newInstance(): DelegateAggregateFunction
}

abstract class DelegateAggregateFunction extends AggregateFunction {
  self: Product =>

  override def eval(input: Row): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  def evaluate(output: MutableRow): Unit
}

case class DelegateSum(child: Expression, delegatee: Delegatee, offset: Int)
  extends DelegateAggregateExpression with trees.UnaryNode[Expression] {

  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType match {
    case DecimalType.Fixed(_, _) =>
      DecimalType.Unlimited
    case _ =>
      child.dataType
  }
  override def toString = s"SUM($child, $delegatee)"

  override def newInstance() = dataType match {
    case LongType => new DelegateSumFunctionLong(child, delegatee.multiplicity, offset, this)
    case DoubleType => new DelegateSumFunctionDouble(child, delegatee.multiplicity, offset, this)
    case _ => new DelegateSumFunction(child, delegatee.multiplicity, offset, this)
  }
}

case class DelegateSumFunctionDouble(
    expr: Expression, multiplicity: Array[Int], offset: Int, base: AggregateExpression)
  extends DelegateAggregateFunction {
  def this() = this(null, null, 0, null) // Required for serialization.

  private[this] val sums = new Array[Double](multiplicity.length)

  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      val primitive: Double = evaluatedExpr.asInstanceOf[Double]
      var i = 0
      while (i < multiplicity.length) {
        (multiplicity(i): @switch) match {
          case 0 =>
          case 1 => sums(i) += primitive
          case n => sums(i) += primitive * n
        }
        i += 1
      }
    }
  }

  override def evaluate(output: MutableRow): Unit = {
    var i = 0
    while (i < sums.length) {
      output(i + offset) = sums(i)
      i += 1
    }
  }
}

case class DelegateSumFunctionLong(
    expr: Expression, multiplicity: Array[Int], offset: Int, base: AggregateExpression)
  extends DelegateAggregateFunction {
  def this() = this(null, null, 0, null) // Required for serialization.

  private[this] val sums = new Array[Long](multiplicity.length)

  override def update(input: Row): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      val primitive: Long = evaluatedExpr.asInstanceOf[Long]
      var i = 0
      while (i < multiplicity.length) {
        (multiplicity(i): @switch) match {
          case 0 =>
          case 1 => sums(i) += primitive
          case n => sums(i) += primitive * n
        }
        i += 1
      }
    }
  }

  override def evaluate(output: MutableRow): Unit = {
    var i = 0
    while (i < sums.length) {
      output(i + offset) = sums(i)
      i += 1
    }
  }
}


case class DelegateSumFunction(
    expr: Expression, multiplicity: Array[Int], offset: Int, base: AggregateExpression)
  extends DelegateAggregateFunction {
  def this() = this(null, null, 0, null) // Required for serialization.

  private[this] val calcType =
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        DecimalType.Unlimited
      case _ =>
        expr.dataType
    }

  private[this] val numeric = calcType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case _ => sys.error(s"Type does not support numeric operations")
  }

  private[this] val sums = {
    val zero = Cast(Literal(0), calcType).eval(EmptyRow)
    val array = new Array[Any](multiplicity.length)
    var i = 0
    while (i < array.length) {
      array(i) = zero
      i += 1
    }
    array
  }

  private[this] val castExpr = if (expr.dataType == calcType) expr else Cast(expr, calcType)

  override def update(input: Row): Unit = {
    val evaluatedExpr = castExpr.eval(input)
    if (evaluatedExpr != null) {
      var i = 0
      while (i < multiplicity.length) {
        (multiplicity(i): @switch) match {
          case 0 =>
          case 1 =>
            sums(i) = numeric.plus(sums(i), evaluatedExpr)
          case 2 =>
            sums(i) = numeric.plus(sums(i), numeric.plus(evaluatedExpr, evaluatedExpr))
          case n =>
            sums(i) = numeric.plus(sums(i), numeric.times(evaluatedExpr, numeric.fromInt(n)))
        }
        i += 1
      }
    }
  }

  override def evaluate(output: MutableRow): Unit = {
    expr.dataType match {
      case DecimalType.Fixed(_, _) =>
        val literal = MutableLiteral(null, calcType)
        val cast = Cast(literal, dataType)
        var i = 0
        while (i < sums.length) {
          literal.value = sums(i)
          output(i + offset) = cast.eval(EmptyRow)
          i += 1
        }
      case _ =>
        var i = 0
        while (i < sums.length) {
          output(i + offset) = sums(i)
          i += 1
        }
    }
  }
}

case class DelegateCount(child: Expression, delegatee: Delegatee, offset: Int)
  extends DelegateAggregateExpression with trees.UnaryNode[Expression] {

  override def nullable = false
  override def dataType: DataType = LongType
  override def toString = s"COUNT($child, $delegatee)"

  override def newInstance() = new DelegateCountFunction(child, delegatee.multiplicity, offset, this)
}

case class DelegateCountFunction(
    expr: Expression, multiplicity: Array[Int], offset: Int, base: AggregateExpression)
  extends DelegateAggregateFunction {
  def this() = this(null, null, 0, null) // Required for serialization.

  private[this] val counts = new Array[Long](multiplicity.length)

  override def update(input: Row): Unit = {
    if (expr.eval(input) != null) {
      var i = 0
      while (i < multiplicity.length) {
        counts(i) += multiplicity(i).toLong
        i += 1
      }
    }
  }

  override def evaluate(output: MutableRow): Unit = {
    var i = 0
    while (i < counts.length) {
      output(i + offset) = counts(i)
      i += 1
    }
  }
}

case class DelegateCount01(child: Expression, delegatee: Delegatee, offset: Int)
  extends DelegateAggregateExpression with trees.UnaryNode[Expression] {
  require(child.dataType == BooleanType && !child.nullable,
    s"!!! Warning: $child has unexpected child type ${child.dataType}")

  override def nullable = false
  override def dataType: DataType = ByteType
  override def toString = s"COUNT01($child, $delegatee)"

  override def newInstance() =
    new DelegateCount01Function(child, delegatee.multiplicity, offset, this)
}

case class DelegateCount01Function(
    expr: Expression,
    multiplicity: Array[Int],
    offset: Int,
    base: AggregateExpression)
  extends DelegateAggregateFunction {
  def this() = this(null, null, 0, null) // Required for serialization.

  private[this] val seens = new Array[Boolean](multiplicity.length)
  private[this] var index = 0

  override def update(input: Row): Unit = {
    if (index < multiplicity.length) {
      if (expr.eval(input).asInstanceOf[Boolean]) {
        while (index < multiplicity.length && seens(index)) {
          index += 1
        }
        var i = index
        while (i < multiplicity.length) {
          if (multiplicity(i) != 0) seens(i) = true
          i += 1
        }
      }
    }
  }

  override def evaluate(output: MutableRow): Unit = {
    var i = 0
    while (i < seens.length) {
      output(i + offset) = if (seens(i)) 1.toByte else 0.toByte
      i += 1
    }
  }
}

case class Delegatee(ordinals: Array[Int]) extends DelegateAggregateFunction {

  override def toString = s"Delegatee(${ordinals.mkString("[", ",", "]")})"

  val multiplicity = new Array[Int](ordinals.length)

  override def update(input: Row): Unit = {
    var i = 0
    while (i < ordinals.length) {
      multiplicity(i) = input.getByte(ordinals(i)) & 0xFF
      i += 1
    }
  }

  override def evaluate(output: MutableRow): Unit =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override val base: AggregateExpression = null
}


case class SparseMutableRow(indexes: Array[Int]) extends MutableRow {
  private[this] var row: MutableRow = _

  def apply(newRow: Row): Row = {
    row = newRow.asInstanceOf[MutableRow]
    this
  }

  override def length: Int = indexes.length

  def apply(i: Int): Any = row(indexes(i))

  override def setNullAt(i: Int): Unit = row.setNullAt(indexes(i))
  override def isNullAt(i: Int): Boolean = row.isNullAt(indexes(i))

  override def getInt(i: Int): Int = row.getInt(indexes(i))
  override def getShort(i: Int): Short = row.getShort(indexes(i))
  override def getBoolean(i: Int): Boolean = row.getBoolean(indexes(i))
  override def getByte(i: Int): Byte = row.getByte(indexes(i))
  override def getLong(i: Int): Long = row.getLong(indexes(i))
  override def getFloat(i: Int): Float = row.getFloat(indexes(i))
  override def getDouble(i: Int): Double = row.getDouble(indexes(i))
  override def getString(i: Int): String = row.getString(indexes(i))

  override def update(i: Int, value: Any) = row.update(indexes(i), value)

  override def setInt(i: Int, value: Int) = row.setInt(indexes(i), value)
  override def setLong(i: Int, value: Long) = row.setLong(indexes(i), value)
  override def setDouble(i: Int, value: Double) = row.setDouble(indexes(i), value)
  override def setBoolean(i: Int, value: Boolean) = row.setBoolean(indexes(i), value)
  override def setShort(i: Int, value: Short) = row.setShort(indexes(i), value)
  override def setByte(i: Int, value: Byte) = row.setByte(indexes(i), value)
  override def setFloat(i: Int, value: Float) = row.setFloat(indexes(i), value)
  override def setString(i: Int, value: String) = row.setString(indexes(i), value)

  override def copy(): Row = new GenericRow(indexes.map(row.apply))

  override def toSeq: Seq[Any] = indexes.map(row.apply).toSeq

  override def toString(): String = toSeq.mkString("[", ",", "]")
}

case class ApproxColumn(
    confidence: Double,
    columns: Seq[Expression],
    multiplicities: Seq[Expression],
    finalBatch: Boolean = false)
  extends Expression with Logging {

  override type EvaluatedType = Any

  override def nullable: Boolean = false

  override lazy val resolved = childrenResolved &&
    columns.map(_.dataType).distinct.size == 1 &&
    columns.head.dataType.isInstanceOf[NumericType] &&
    multiplicities.map(_.dataType).forall(_ == ByteType)

  override def dataType: DataType = {
    val eType = columns.head.dataType
    val eNull = columns.head.nullable
    StructType(
      StructField("point_estimate", eType, eNull) ::
        StructField("conf_inv_lower", eType, eNull) ::
        StructField("conf_inv_upper", eType, eNull) ::
        Nil
    )
  }

  override def children: Seq[Expression] = columns ++ multiplicities

  override def toString = s"Approx(${columns.mkString(",")})"

  private[this] val exprArray = multiplicities.zip(columns).toArray
  private[this] val lower = (1 - confidence) / 2
  private[this] val upper = (1 + confidence) / 2

  private[this] val numeric =
    if (resolved) {
      columns.head.dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Numeric[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Numeric[Any]]
    }

  override def eval(input: Row): Any = {
    val values = exprArray.collect {
      case (mult, col) if mult.eval(input).asInstanceOf[Byte] != 0.toByte => col.eval(input)
    }
    val sorted = values.sorted(numeric)

    val row = new GenericMutableRow(3)
    row(0) = values(0)
    if (finalBatch) {
      row(1) = values(0)
      row(2) = values(0)
    } else {
      row(1) = sorted((values.length * lower).floor.toInt)
      row(2) = sorted((values.length * upper).ceil.toInt - 1)
//      row(0) = values.length.asInstanceOf[Double]
      var sortedOut = ""
      sorted.foreach { x => sortedOut += x + ","}
      logInfo(s"LOGAN sorted: $sortedOut\n")
    }
    row
  }
}
