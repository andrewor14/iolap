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

import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.columnar.{InMemoryColumnarTableScan, InMemoryRelation}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.hive.execution.HiveTableScan
import org.apache.spark.sql.hive.online.OnlinePlannerUtil._
import org.apache.spark.sql.hive.online.joins._
import org.apache.spark.sql.types.{BooleanType, DataType}

import scala.collection.mutable

class OnlineSQLPlanner(conf: OnlineSQLConf, controller: OnlineDataFrame)
  extends RuleExecutor[SparkPlan] {
  protected val batches =
    Batch("Identify Sampled Relations", Once,
      SafetyCheck,
      IdentifyStreamedRelations(conf.streamedRelations, controller)
    ) ::
    Batch("Pre-Bootstrap Optimization", FixedPoint(100),
      PruneProjects
    ) ::
    Batch("Bootstrap", Once,
      PushDownPartialAggregate,
      PushUpResample,
      PushUpSeed,
      ImplementResample,
      PropagateBootstrap,
      IdentifyUncertainTuples,
      CleanupOutputTuples,
      InsertCollect(conf.isDebug, conf.estimationConfidence)
    ) ::
    Batch("Post-Bootstrap Optimization", FixedPoint(100),
      PruneColumns,
      PushDownFilter,
      PruneProjects,
      OptimizeOperatorOrder,
      PruneFilters
    ) ::
    Batch("Consolidate Bootstrap & Lineage Embedding", Once,
      ConsolidateBootstrap(conf.numBootstrapTrials),
      IdentifyLazyEvaluates,
      EmbedLineage
    ) ::
    Batch("Materialize Plan", Once,
      ImplementSort,
      ImplementJoin(controller),
      ImplementProject(controller),
      ImplementAggregate(conf.variationRangeSlack, controller),
      ImplementCollect(controller),
      CleanupAnalysisExpressions
    ) ::
    Nil
}

/**
 * Remark:
 * 1. Online planning should be applied before inserting exchange
 * (and thus we exclude exchange here),
 *  because:
 *  (1) ImplementJoin will change join types, which will affect exchange;
 *  (2) Pushing resampling above exchange is always subsumed by pushing it above join.
 *    Note that pushing above exchange/join will change the order of tuples,
 *    and thus cannot be applied to multi-instance relations.
 * 2. Turn off LeftSemiJoinHash for now, using Join instead.
 */
object SafetyCheck extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case leaf: LeafNode => leaf
    case filter: Filter => filter
    case project: Project => project
    case aggregate: Aggregate => aggregate
    case join: BroadcastHashJoin => join
    case join: ShuffledHashJoin => join
    case join: SortMergeJoin => join
    // case join: LeftSemiJoinHash => join
    case sort: ExternalSort => sort
    case _ => ???
  }.transformAllExpressions {
    case alias: Alias =>
      alias.transformChildrenDown {
        case Alias(child, _) => child
      }
  }
}

case class IdentifyStreamedRelations(streamed: Set[String], controller: OnlineDataFrame)
  extends Rule[SparkPlan] {

  private[this] val PARTITION_ERROR =
    "We do not support streaming through partitioned relations for now."

  private[this] val map = new mutable.HashMap[String, RelationReference]()

  override def apply(plan: SparkPlan): SparkPlan = {
      logInfo(s"LOGAN: streamed: ${streamed.toArray.mkString(",")}" +
        s" ${plan.getClass.getCanonicalName()}")
      plan.transformUp {
      case scan@PhysicalRDD(_, rdd) if streamed.contains(rdd.name) =>
        logInfo(s"LOGAN: A")
        val reference = getOrNewRef(rdd.name, rdd.partitions.length)
        StreamedRelation(withSeed(scan, reference))(reference, controller)

      case scan@HiveTableScan(_, r, _) if streamed.contains(r.tableName) =>
        logInfo(s"LOGAN: B")
        require(!r.hiveQlTable.isPartitioned, PARTITION_ERROR)
        val reference = getOrNewRef(r.tableName, scan.execute().partitions.length)
        StreamedRelation(withSeed(scan, reference))(reference, controller)

      case scan@InMemoryColumnarTableScan(_, _, r@InMemoryRelation(_, _, _, _, _, Some(tableName)))
        if streamed.contains(tableName) =>
        logInfo(s"LOGAN: C")
        val reference = getOrNewRef(tableName, r.cachedColumnBuffers.partitions.length)
        StreamedRelation(withSeed(scan, reference))(reference, controller)

      case scan: LeafNode =>
        logInfo(s"LOGAN: D")
        OTStreamedRelation(scan)(controller)
    }
  }

  private[this] def withSeed(plan: SparkPlan, ref: RelationReference) =
    Project(plan.output :+ TaggedAlias(Seed(ref), RandomSeed(), OnlineSQLConf.SEED_COLUMN)(), plan)

  private[this] def getOrNewRef(tableName: String, numPartitions: Int) = {
    val reference = map.getOrElse(tableName, {
      val id = RelationReference.newReference(numPartitions)
      map(tableName) = id
      controller.registerRelation(id)
      id
    })
    reference.reference()
    reference
  }
}

case class ResamplePlaceholder(
    branches: Seq[RelationReference], child: SparkPlan) extends UnaryNode {
  def numBranches: Int = branches.length
  override def output = child.output
  override def doExecute() =
    throw new TreeNodeException(this, s"No function to execute plan. type: ${this.nodeName}")
}

/**
 * Insert resampling operator at the optimal place.
 * Remark:
 *  1. Optimize by pushing ResamplePlaceholder above Filter, Project.
 *  2. This optimization is valid because ResamplePlaceholder is only used
 *    for tuples from base relations. Uncertain attributes are generated by aggregates
 *    which have to go through join in order to be combined,
 *    and we will merge the bootstrap-counts columns into one immediately after joins.
 *  3. We cannot push ResamplePlaceholder above Join.
 *    (TODO: Unless we know this is a foreign key join.)
 */
object PushUpResample extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case stream: StreamedRelation => ResamplePlaceholder(Seq(stream.reference), stream)

    case Filter(condition, ResamplePlaceholder(b, child)) =>
      ResamplePlaceholder(b, Filter(condition, child))

    case Project(projectList, ResamplePlaceholder(b, child)) =>
      ResamplePlaceholder(b, Project(projectList ++ getSeeds(child.output), child))

    case BroadcastHashJoin(leftKeys, rightKeys, BuildLeft,
          left: ResamplePlaceholder, ResamplePlaceholder(b, right)) =>
      ResamplePlaceholder(b, BroadcastHashJoin(leftKeys, rightKeys, BuildLeft, left, right))
    case BroadcastHashJoin(leftKeys, rightKeys, BuildRight,
          ResamplePlaceholder(b, left), right: ResamplePlaceholder) =>
      ResamplePlaceholder(b, BroadcastHashJoin(leftKeys, rightKeys, BuildRight, left, right))
    case BroadcastHashJoin(leftKeys, rightKeys, buildSide, ResamplePlaceholder(b, left), right) =>
      ResamplePlaceholder(b, BroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right))
    case BroadcastHashJoin(leftKeys, rightKeys, buildSide, left, ResamplePlaceholder(b, right)) =>
      ResamplePlaceholder(b, BroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right))

    case ShuffledHashJoin(leftKeys, rightKeys, BuildLeft,
          left: ResamplePlaceholder, ResamplePlaceholder(b, right)) =>
      ResamplePlaceholder(b, ShuffledHashJoin(leftKeys, rightKeys, BuildLeft, left, right))
    case ShuffledHashJoin(leftKeys, rightKeys, BuildRight,
          ResamplePlaceholder(b, left), right: ResamplePlaceholder) =>
      ResamplePlaceholder(b, ShuffledHashJoin(leftKeys, rightKeys, BuildRight, left, right))
    case ShuffledHashJoin(leftKeys, rightKeys, buildSide, ResamplePlaceholder(b, left), right) =>
      ResamplePlaceholder(b, ShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right))
    case ShuffledHashJoin(leftKeys, rightKeys, buildSide, left, ResamplePlaceholder(b, right)) =>
      ResamplePlaceholder(b, ShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right))

    case SortMergeJoin(leftKeys, rightKeys, ResamplePlaceholder(b, left), right) =>
      ResamplePlaceholder(b, SortMergeJoin(leftKeys, rightKeys, left, right))
    case SortMergeJoin(leftKeys, rightKeys, left, ResamplePlaceholder(b, right)) =>
      ResamplePlaceholder(b, SortMergeJoin(leftKeys, rightKeys, left, right))

    case LeftSemiJoinHash(leftKeys, rightKeys, ResamplePlaceholder(b, left), right) =>
      ResamplePlaceholder(b, LeftSemiJoinHash(leftKeys, rightKeys, left, right))

    case ExternalSort(sortOrder, global, ResamplePlaceholder(b, child)) =>
      ResamplePlaceholder(b, ExternalSort(sortOrder, global, child))
  }
}

/**
 * Postpone materializing seed.
 */
object PushUpSeed extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case stream@StreamedRelation(Project(projectList, child)) =>
      val (seeds, others) = projectList.partition {
        case TaggedAlias(Seed(ref), _, _) if !ref.multiRef => true
        case TaggedAttribute(Seed(ref), _, _, _, _) if !ref.multiRef => true
        case _ => false
      }
      val stream2 = StreamedRelation(Project(others, child))(
        stream.reference, stream.controller, stream.trace)
      Project(stream2.output ++ seeds, stream2)

    case Project(projectList1, Project(projectList2, child)) =>
      // This should be efficient,
      // as we don't manage bootstrap-counts in Project except after joins.
      val map = projectList2.map(named => (named.exprId, named)).toMap
      val projectList = projectList1.map {
        case attr: Attribute => map(attr.exprId)
        case named => named.transformChildrenUp {
          case attr: Attribute => map(attr.exprId)
        }.transformChildrenUp {
          case alias: Alias => alias.child
          case alias: TaggedAlias => alias.child
        }
      }
      Project(projectList, child)

    case filter@Filter(condition, Project(projectList, child)) =>
      val (seeds, others) = projectList.partition {
        case TaggedAlias(Seed(ref), _, _) if !ref.multiRef => true
        case TaggedAttribute(Seed(ref), _, _, _, _) if !ref.multiRef => true
        case _ => false
      }
      if (AttributeSet(seeds.map(_.toAttribute)).intersect(condition.references).isEmpty) {
        val filter2 = Filter(condition, Project(others, child))
        Project(filter2.output ++ seeds, filter2)
      } else {
        filter
      }
  }
}

/**
 *  Partial-Aggregate
 *    Join
 *      Stem -- Computes all partial aggregates references
 *      Filter-Branch -- (1) Only computes the join keys, serving as a filter
 *                       (2) Tuples may be uncertain
 *                        (i.e., has uncertain flag after IdentifyUncertainTuples)
 *
 * is optimized into:
 *
 *  Join
 *    Partial-Aggregate
 *      Stem
 *    Branch
 */
object PushDownPartialAggregate extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case aggregate@Aggregate(true, groupings, aggrs,
      BroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right)) =>
      val references = (groupings ++ aggrs).map(_.references).reduceOption(_ ++ _)
        .getOrElse(AttributeSet(Nil))

      if (references.subsetOf(right.outputSet) && isFilterBranch(left)) {
        val groupKeys = (groupings ++ rightKeys.flatMap(_.references)).distinct
        val aggregates = (aggrs ++ rightKeys.flatMap(_.references)).distinct
        BroadcastHashJoin(leftKeys, rightKeys, buildSide,
          left, Aggregate(partial = true, groupKeys, aggregates, right))
      } else if (references.subsetOf(left.outputSet) && isFilterBranch(right)) {
        val groupKeys = (groupings ++ leftKeys.flatMap(_.references)).distinct
        val aggregates = (aggrs ++ leftKeys.flatMap(_.references)).distinct
        BroadcastHashJoin(leftKeys, rightKeys, buildSide,
          Aggregate(partial = true, groupKeys, aggregates, left), right)
      } else {
        aggregate
      }

    case aggregate@Aggregate(true, groupings, aggrs,
      ShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right)) =>
      val references = (groupings ++ aggrs).map(_.references).reduce(_ ++ _)

      if (references.subsetOf(right.outputSet) && isFilterBranch(left)) {
        val groupKeys = (groupings ++ rightKeys.flatMap(_.references)).distinct
        val aggregates = (aggrs ++ rightKeys.flatMap(_.references)).distinct
        ShuffledHashJoin(leftKeys, rightKeys, buildSide,
          left, Aggregate(partial = true, groupKeys, aggregates, right))
      } else if (references.subsetOf(left.outputSet) && isFilterBranch(right)) {
        val groupKeys = (groupings ++ leftKeys.flatMap(_.references)).distinct
        val aggregates = (aggrs ++ leftKeys.flatMap(_.references)).distinct
        ShuffledHashJoin(leftKeys, rightKeys, buildSide,
          Aggregate(partial = true, groupKeys, aggregates, left), right)
      } else {
        aggregate
      }

    case aggregate@Aggregate(true, groupings, aggrs,
      SortMergeJoin(leftKeys, rightKeys, left, right)) =>
      val references = (groupings ++ aggrs).map(_.references).reduce(_ ++ _)

      if (references.subsetOf(right.outputSet) && isFilterBranch(left)) {
        val groupKeys = (groupings ++ rightKeys.flatMap(_.references)).distinct
        val aggregates = (aggrs ++ rightKeys.flatMap(_.references)).distinct
        SortMergeJoin(leftKeys, rightKeys,
          left, Aggregate(partial = true, groupKeys, aggregates, right))
      } else if (references.subsetOf(left.outputSet) && isFilterBranch(right)) {
        val groupKeys = (groupings ++ leftKeys.flatMap(_.references)).distinct
        val aggregates = (aggrs ++ leftKeys.flatMap(_.references)).distinct
        SortMergeJoin(leftKeys, rightKeys,
          Aggregate(partial = true, groupKeys, aggregates, left), right)
      } else {
        aggregate
      }
  }

  def intersect(expr: Expression, attrs: AttributeSet): Boolean =
    expr.references.intersect(attrs).nonEmpty

  def intersect(exprs: Seq[Expression], attrs: AttributeSet*): Boolean =
    exprs.exists(e => attrs.exists(a => intersect(e, a)))

  def aggregations(expr: Expression) = expr.collect { case a: AggregateExpression => a}

  def isFilterBranch(plan: SparkPlan): Boolean = {
    var smplAttr = AttributeSet(Seq())
    var uncAttr = AttributeSet(Seq())
    var flag = false

    plan.transformUp {
      case resample: ResamplePlaceholder =>
        smplAttr = smplAttr ++ AttributeSet(resample.output)
        resample
      case project@Project(projectList, child) =>
        smplAttr = smplAttr ++
          AttributeSet(projectList.collect { case e if intersect(e, smplAttr) => e.toAttribute})
        uncAttr = uncAttr ++
          AttributeSet(projectList.collect { case e if intersect(e, uncAttr) => e.toAttribute})
        project
      case aggregate@Aggregate(_, _, aggrs, _) =>
        smplAttr = smplAttr ++
          AttributeSet(aggrs.collect {
            case e if !intersect(aggregations(e), smplAttr, uncAttr) => e.toAttribute
          })
        uncAttr = uncAttr ++
          AttributeSet(aggrs.collect {
            case e if intersect(aggregations(e), smplAttr, uncAttr) => e.toAttribute
          })
        aggregate
      case filter@Filter(condition, _) if intersect(condition, uncAttr) =>
        flag = true
        filter
    }

    flag
  }
}

/**
 * Replace the resampling operator placeholder with its actual implementation.
 */
object ImplementResample extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case rs@ResamplePlaceholder(branches, child) =>
      require(rs.numBranches == 1, "Unexpected multiple seeds under existing assumptions.")
      val (others, seeds) = child.output.partition {
        case TaggedAttribute(_: Seed, _, _, _, _) => false
        case _ => true
      }
      val btCnt = TaggedAlias(Bootstrap(branches),
        seeds.map(s => SetSeedAndPoisson(s, Poisson())).reduce(Multiply), "_btcnt")()
      Project(others :+ btCnt, child)
  }
}

case class LowerPlaceholder(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override def foldable = child.foldable
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType
  override def toString = s"Lower($child)"

  override def eval(input: Row): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")
}

case class UpperPlaceholder(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override def foldable = child.foldable
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType
  override def toString = s"Upper($child)"

  override def eval(input: Row): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")
}

/**
 * Propagate reampling through the plan, and insert uncertain attributes, e.g.
 * 1. Propagate bootstrap-counts
 * 2. Aggregates should take in account of bootstrap-counts
 * 3. Join should multiply bootstrap-counts columns
 * 4. Filter should update bootstrap-counts
 *  NOTE: handled after uncertain attributes are identified
 * 5. Uncertain attributes are inferred based on the assumption of
 *  no partitioning in the input relations.
 */
object PropagateBootstrap extends Rule[SparkPlan] {
  import BoundType._

  private[this] val JOIN_KEY_ERROR = "Join keys cannot be uncertain columns."
  private[this] val GROUP_BY_KEY_ERROR = "Group-by keys cannot be uncertain columns."
  private[this] val SORT_KEY_ERROR = "Sort keys cannot be uncertain columns."

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case q: SparkPlan =>
      val withNewLeaves: SparkPlan = q.transformExpressionsUp {
        case attr: Attribute => q.inputSet.find(_.exprId == attr.exprId).getOrElse(attr)
      }

      val transformed = withNewLeaves match {
        case project@Project(projectList, child) =>
          getBtCnt(child.output) match {
            case Some(btCnt@TaggedAttribute(Bootstrap(se1), _, _, _, _)) =>
              var seenBtCnt = false
              val toBounds = extractBounds(child.output)
              val updated = projectList.flatMap {
                case alias@TaggedAlias(Bootstrap(se2), expr, name) =>
                  seenBtCnt = true
                  TaggedAlias(Bootstrap(se2 ++ se1), ByteMultiply(btCnt, expr), name)(
                    alias.exprId, alias.qualifiers) :: Nil
                case alias@Alias(expr, name) if containsUncertain(expr.references) =>
                  TaggedAlias(Uncertain, expr, name)(alias.exprId, alias.qualifiers) +:
                  explode(expr, toBounds).map { e =>
                    TaggedAlias(Bound(Mixed, alias.exprId), e, name)(
                      qualifiers = alias.qualifiers)
                  }
                case attr@TaggedAttribute(Uncertain, _, _, _, _) =>
                  attr +: toBounds(attr.exprId)
                case expr => expr :: Nil
              }
              val newProjectList = if (seenBtCnt) updated else updated :+ btCnt
              Project(newProjectList, child)
            case _ => project
          }

        case aggregate@Aggregate(partial, groupings, aggrs, child) =>
          require(deterministicKeys(groupings), GROUP_BY_KEY_ERROR)
          getBtCnt(child.output) match {
            case Some(btCnt@TaggedAttribute(Bootstrap(b), _, _, _, _)) =>
              val params = aggrs.flatMap {
                _.collect { case a: AggregateExpression => a.children }.flatten
              }.distinct

              val (optimized, newChild) =
                if (params.forall { case _: Attribute | _: Literal => true case _ => false }) {
                  (aggrs, child)
                } else {
                  val map = params.flatMap {
                    case n: NamedExpression => Some((n, n))
                    case _: Literal => None
                    case e => Some((e, Alias(e, "_param")()))
                  }.toMap
                  val projectList =
                    (groupings.flatMap(_.references) ++ map.values :+ btCnt).distinct
                  val consolidated = aggrs.map {
                    _.transformChildrenUp { case a: AggregateExpression =>
                      a.mapChildren {
                        case e if map.contains(e) => map(e).toAttribute
                        case e => e
                      }
                    }
                  }
                  (consolidated, Project(projectList, child))
                }

              val aBtCnt = TaggedAlias(Bootstrap(if (partial) b else Seq()),
                Delegate(btCnt, Count01(boolTrue)), "_btcnt")()
              val newAggrs = optimized.flatMap {
                case alias@Alias(expr, name) if isAggregate(expr) =>
                  val newExpr = expr.transformUp {
                    case sum: Sum =>
                      val dSum = if (partial || b.isEmpty) {
                        Delegate(btCnt, sum)
                      } else {
                        Cast(scale(Delegate(btCnt, sum), ScaleFactor(b)), sum.dataType)
                      }
                      If(ByteNonZero(aBtCnt.child), dSum, Literal.create(null, dSum.dataType))

                    case CombineSum(e) =>
                      val sum = Sum(e) // TODO: fix this hack
                      val dSum = if (partial || b.isEmpty) {
                        Delegate(btCnt, sum)
                      } else {
                        Cast(scale(Delegate(btCnt, sum), ScaleFactor(b)), sum.dataType)
                      }
                      If(ByteNonZero(aBtCnt.child), dSum, Literal.create(null, dSum.dataType))

                    case count: Count =>
                      if (partial || b.isEmpty) {
                        Delegate(btCnt, count)
                      } else {
                        Cast(scale(Delegate(btCnt, count), ScaleFactor(b)), count.dataType)
                      }

                    case _: AggregateExpression => ??? // There should not be any Average
                  }
                  if (!partial) {
                    TaggedAlias(Uncertain, newExpr, name)(alias.exprId, alias.qualifiers) ::
                    TaggedAlias(Bound(Lower, alias.exprId), LowerPlaceholder(newExpr), name)(
                      qualifiers = alias.qualifiers) ::
                    TaggedAlias(Bound(Upper, alias.exprId), UpperPlaceholder(newExpr), name)(
                      qualifiers = alias.qualifiers) ::
                    Nil
                  } else if (containsUncertain(expr.references)) {
                    TaggedAlias(Uncertain, newExpr, name)(alias.exprId, alias.qualifiers) :: Nil
                  } else {
                    Alias(newExpr, name)(alias.exprId, alias.qualifiers) :: Nil
                  }

                case expr => expr :: Nil
              } :+ aBtCnt

              BootstrapAggregate(partial, groupings, newAggrs, newChild)

            case _ => aggregate
          }

        case join@BroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right) =>
          // TODO: move uncertain join keys to a filter
          require(deterministicKeys(leftKeys) && deterministicKeys(rightKeys), JOIN_KEY_ERROR)
          postprocess(join)

        case join@ShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right) =>
          require(deterministicKeys(leftKeys) && deterministicKeys(rightKeys), JOIN_KEY_ERROR)
          postprocess(join)

        case join@SortMergeJoin(leftKeys, rightKeys, left, right) =>
          require(deterministicKeys(leftKeys) && deterministicKeys(rightKeys), JOIN_KEY_ERROR)
          postprocess(join)

        case join@LeftSemiJoinHash(leftKeys, rightKeys, left, right) =>
          require(deterministicKeys(leftKeys) && deterministicKeys(rightKeys), JOIN_KEY_ERROR)
          // Distinct the right branch
          val newJoin = getBtCnt(right.output) match {
            case Some(btCnt@TaggedAttribute(Bootstrap(b), _, _, _, _)) if b.nonEmpty =>
              val newBtCnt = TaggedAlias(Bootstrap(Seq()),
                Delegate(btCnt, Count01(boolTrue)), "_btcnt")()
              val newRight = BootstrapAggregate(partial = false, rightKeys,
                toNamed(rightKeys) :+ newBtCnt, right)
              LeftSemiJoinHash(leftKeys, rightKeys, left, newRight)
            case None => join
          }
          postprocess(newJoin)

        case sort@ExternalSort(sortOrder, global, child) =>
          require(deterministicKeys(sortOrder), SORT_KEY_ERROR)
          sort

        case other => other
      }

      // Simplify cast
      transformed.transformExpressionsUp(simplifyCast)
  }

  def deterministicKeys(keys: Seq[Expression]): Boolean =
    keys.forall(key => !containsUncertain(key.references))

  def isAggregate(expr: Expression): Boolean = expr match {
    case _: AggregateExpression => true
    case _ => expr.children.exists(isAggregate)
  }

  def scale(expr: Expression, factor: Expression) = {
    val (left, right) = widenTypes(expr, factor)
    Multiply(left, right)
  }

  def postprocess(plan: SparkPlan): SparkPlan =
    plan.output.partition {
      case TaggedAttribute(_: Bootstrap, _, _, _, _) => true
      case _ => false
    } match {
      case (Seq(a@TaggedAttribute(Bootstrap(b1), _, _, _, _),
        b@TaggedAttribute(Bootstrap(b2), _, _, _, _)), others) =>
        Project(others :+ TaggedAlias(Bootstrap(b1 ++ b2), ByteMultiply(a, b), "_btcnt")(), plan)
      case _ => plan
    }
}

case class ExistsPlaceholder(child: Expression) extends UnaryExpression with Predicate {
  override def foldable = child.foldable
  override def nullable: Boolean = child.nullable

  override def toString = s"Exists($child)"

  override def eval(input: Row): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")
}

case class ForAllPlaceholder(child: Expression) extends UnaryExpression with Predicate {
  override def foldable = child.foldable
  def nullable: Boolean = child.nullable

  override def toString = s"ForAll($child)"

  override def eval(input: Row): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")
}

/**
 * Insert uncertain flag.
 * Remark:
 *  1. Uncertainty flags are inserted to optimize caching.
 *  2. Any case where the bootstrap-counts may change is flagged uncertain.
 */
object IdentifyUncertainTuples extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case Filter(condition, child) if containsUncertain(condition.references) =>
      val checks = explode(condition, extractBounds(child.output))
      val flag = checks.tail.map { check =>
        Coalesce(EqualTo(check, checks.head) :: boolFalse :: Nil)
      }.reduce(And)
      val projectList = child.output.collect {
        case a@TaggedAttribute(tag: Bootstrap, _, _, _, _) =>
          TaggedAlias(tag, If(condition, a, byte0), "_btcnt")(a.exprId, a.qualifiers)
        case a@TaggedAttribute(Flag, _, _, _, _) =>
          TaggedAlias(Flag, And(a, flag), "_flag")(a.exprId, a.qualifiers)
        case a => a
      } ++ {
        getFlag(child.output) match {
          case Some(_) => None
          case None => Some(TaggedAlias(Flag, flag, "_flag")())
        }
      }
      val filter = ExistsPlaceholder(ByteNonZero(getBtCnt(projectList.map(_.toAttribute)).get))
      Filter(filter, Project(projectList, child))

    case project@Project(projectList, child) =>
      Project(projectList ++ getFlag(child.output), child)

    case aggregate@BootstrapAggregate(partial, groupings, aggrs, child) =>
      val btCnt = getBtCnt(child.output).get
      if (partial) {
        getFlag(child.output) match {
          case Some(flag) => BootstrapAggregate(partial, groupings :+ flag, aggrs :+ flag, child)
          case _ => aggregate
        }
      } else {
        val flag = getFlag(child.output) match {
          case Some(f) =>
            TaggedAlias(Flag,
              ForAllPlaceholder(ByteNonZero(Delegate(btCnt, Count01(f)))), "_flag")()
          case _ =>
            TaggedAlias(Flag,
              ForAllPlaceholder(ByteNonZero(Delegate(btCnt, Count01(boolTrue)))),
              "_flag")()
        }
        BootstrapAggregate(partial, groupings, aggrs :+ flag, child)
      }

    case join@BroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right) =>
      postprocess(join)

    case join@ShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right) =>
      postprocess(join)

    case join@SortMergeJoin(leftKeys, rightKeys, left, right) =>
      postprocess(join)

    case join@LeftSemiJoinHash(leftKeys, rightKeys, left, right) =>
      postprocess(join)

    // Hack: turn this off because we do not support "order by",
    //  but do need external sort for sort merge join.
    //  For now, sort merge join always do not grow on the cache side,
    //  and do not cache on the non-cache side,
    //  so this hack is correct
    //    case sort@ExternalSort(sortOrder, global, child) =>
    //      getFlag(child.output) match {
    //        case None => sort
    //        case Some(flag) =>
    //          val projectList = child.output.map {
    //            case a@TaggedAttribute(Flag, _, _, _, _) =>
    //              TaggedAlias(Flag, And(boolFalse, a), "_flag")(a.exprId, a.qualifiers)
    //            case a => a
    //          }
    //          ExternalSort(sortOrder, global, Project(projectList, child))
    //      }
  }

  def postprocess(plan: SparkPlan): SparkPlan =
    plan.output.partition {
      case TaggedAttribute(Flag, _, _, _, _) => true
      case _ => false
    } match {
      case (Seq(f1@TaggedAttribute(Flag, _, _, _, _),
        f2@TaggedAttribute(Flag, _, _, _, _)), others) =>
        Project(others :+ TaggedAlias(Flag, And(f1, f2), "_flag")(), plan)
      case _ => plan
    }
}

object CleanupOutputTuples extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = getBtCnt(plan.output) match {
    case Some(b) => Filter(ExistsPlaceholder(ByteNonZero(b)), plan)
    case None => plan
  }
}

case class CollectPlaceholder(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryNode {

  override def references: AttributeSet = super.references ++ AttributeSet(getFlag(child.output))
  override def output = projectList.map(_.toAttribute)
  override def doExecute() =
    throw new TreeNodeException(this, s"No function to execute plan. type: ${this.nodeName}")
}

case class InsertCollect(isDebug: Boolean, confidence: Double) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.collect {
    case s: StreamedRelation => s
  }.headOption match {
    case Some(_) =>
      val projectList = getBtCnt(plan.output) match {
        case Some(b) =>
          plan.output.filter {
            case TaggedAttribute(Flag, _, _, _, _) => false
            case TaggedAttribute(_: Bound, _, _, _, _) => false
            case TaggedAttribute(_: Bootstrap, _, _, _, _) => false
            case _ => true
          }.map {
            case a: TaggedAttribute if !isDebug =>
              Alias(ApproxColumn(confidence, a :: Nil, b :: Nil), a.name)(a.exprId, a.qualifiers)
            case a: Attribute => a
          }
        case None => plan.output
      }
      CollectPlaceholder(projectList, plan)
    case None => plan
  }
}

object PruneColumns extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val used = new mutable.HashSet[ExprId]()
    plan.output.foreach {
      case TaggedAttribute(_: Bound, _, _, _, _) =>
      case a => used += a.exprId
    }
    plan.transformDown {
      case Project(projectList, child) =>
        val project = Project(projectList.filter(e => used.contains(e.exprId)), child)
        used ++= project.references.map(_.exprId)
        project

      case BootstrapAggregate(partial, groupings, aggregates, child) =>
        val aggregate = BootstrapAggregate(partial, groupings,
          aggregates.filter(e => used.contains(e.exprId)), child)
        used ++= aggregate.references.map(_.exprId)
        aggregate

      case Aggregate(partial, groupings, aggregates, child) =>
        val aggregate = Aggregate(partial, groupings,
          aggregates.filter(e => used.contains(e.exprId)), child)
        used ++= aggregate.references.map(_.exprId)
        aggregate

      case op =>
        used ++= op.references.map(_.exprId)
        op
    }
  }
}

object PushDownFilter extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case filter@Filter(condition, Project(projectList, child))
      if condition.references.subsetOf(child.outputSet) =>
      val exprIds = condition.references.map(_.exprId).toSet
      val accessed = projectList.filter(ne => exprIds.contains(ne.exprId))
      if (!accessed.forall(_.isInstanceOf[Attribute])) filter
      else Project(projectList, Filter(condition, child))
  }
}

object PruneProjects extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case Project(projectList1, Project(projectList2, child)) =>
      // This should be efficient,
      // as we don't manage bootstrap-counts in Project except after joins.
      val map = projectList2.map(named => (named.exprId, named)).toMap
      val projectList = projectList1.map {
        case attr: Attribute => map(attr.exprId)
        case named => named.transformChildrenUp {
          case attr: Attribute => map(attr.exprId)
        }.transformChildrenUp {
          case alias: Alias => alias.child
          case alias: TaggedAlias => alias.child
        }
      }
      Project(projectList, child)

    case Project(projectList, child) if projectList.toSet == child.output.toSet => child
      
    case Aggregate(partial, groupings, aggregates, Project(projectList, child))
      if simpleCopy(projectList) =>
      Aggregate(partial, groupings, aggregates, child)

    case BootstrapAggregate(partial, groupings, aggregates, Project(projectList, child))
      if simpleCopy(projectList) =>
      BootstrapAggregate(partial, groupings, aggregates, child)

    case CollectPlaceholder(projectList1, Project(projectList2, child))
      if projectList2.forall {
        case _: Attribute => true
        case TaggedAlias(Flag, Literal(false, BooleanType), _) => true
        case _ => false
      } =>
      CollectPlaceholder(projectList1, child)
  }

  def simpleCopy(projectList: Seq[NamedExpression]): Boolean = projectList.forall {
    case _: Attribute => true
    case _ => false
  }
}

object OptimizeOperatorOrder extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case CollectPlaceholder(projectList, ExternalSort(sortOrder, true, child)) =>
      ExternalSort(sortOrder, global = true, CollectPlaceholder(projectList, child))

    case Filter(condition, ExternalSort(sortOrder, global, child)) =>
      ExternalSort(sortOrder, global, Filter(condition, child))
  }
}

object PruneFilters extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case Filter(condition1, Filter(condition2, child)) =>
      val condition =
        if (condition1 == condition2) condition1
        else And(condition1, condition2)
      Filter(condition, child)
  }
}

/**
 * Flatten the whole plan so that all bootstraps are done in one pass.
 * NOTE: we need to propagate all the attributes again
 *  because we changed the output attributes of operators.
 */
case class ConsolidateBootstrap(numTrials: Int) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val toFlattened = new mutable.HashMap[ExprId, Seq[Attribute]]()

    def flatten(expression: Expression): Seq[Expression] = expression match {
      case ApproxColumn(conf, columns, mults, _) =>
        ApproxColumn(conf, columns.flatMap(flatten), mults.flatMap(flatten)) :: Nil
      case ExistsPlaceholder(child) => flatten(child).reduceRight(Or) :: Nil
      case ForAllPlaceholder(child) => flatten(child).reduceRight(And) :: Nil
      case LowerPlaceholder(child) => flatten(child).reduceRight(MinOf) :: Nil
      case UpperPlaceholder(child) => flatten(child).reduceRight(MaxOf) :: Nil
      case attribute: Attribute if toFlattened.contains(attribute.exprId) =>
        toFlattened(attribute.exprId)
      case alias@TaggedAlias(tag: Bootstrap, child, name)
        if !alias.references.exists(ref => toFlattened.contains(ref.exprId)) =>
        val flattened = flatten(child).zipWithIndex.map { case (expr, idx) =>
          if (idx == 0) TaggedAlias(tag, expr, name)(alias.exprId, alias.qualifiers)
          else TaggedAlias(tag, expr, name)(qualifiers = alias.qualifiers)
        }
        toFlattened(alias.exprId) = flattened.map(_.toAttribute)
        flattened
      case alias@TaggedAlias(tag, child, name) =>
        val flattened = flatten(child).zipWithIndex.map { case (expr, idx) =>
          if (idx == 0) TaggedAlias(tag, expr, name)(alias.exprId, alias.qualifiers)
          else TaggedAlias(tag, expr, name)(qualifiers = alias.qualifiers)
        }
        toFlattened(alias.exprId) = flattened.map(_.toAttribute)
        flattened
      case alias@Alias(child, name) =>
        val flattened = flatten(child).zipWithIndex.map { case (expr, idx) =>
          if (idx == 0) Alias(expr, name)(alias.exprId, alias.qualifiers)
          else Alias(expr, name)(qualifiers = alias.qualifiers)
        }
        toFlattened(alias.exprId) = flattened.map(_.toAttribute)
        flattened
      case poisson: SetSeedAndPoisson =>
        poisson +: Seq.tabulate(numTrials - 1)(_ => poisson.poisson)
      case expr if expr.children.nonEmpty =>
        val flattened: Seq[Seq[Expression]] = expr.children.map(flatten)
        val num = flattened.map(_.length).max
        val newChildren = flattened.map {
          seq => if (seq.size != num) seq.padTo(num, seq.head) else seq
        }.transpose
        newChildren.map { c => expr.withNewChildren(c) }
      case other => other :: Nil
    }

    def flattenNamed(namedExpressions: Seq[NamedExpression]): Seq[NamedExpression] =
      namedExpressions.flatMap { case named =>
        flatten(named).asInstanceOf[Seq[NamedExpression]]
      }.map {
        case alias@TaggedAlias(bound: Bound, _, _) =>
          bound.boundAttributes = toFlattened(bound.child)
          alias
        case other => other
      }

    plan.transformUp {
      case BootstrapAggregate(partial, groupings, aggrs, child) =>
        BootstrapAggregate(partial, groupings, flattenNamed(aggrs), child)

      case Filter(condition, child) =>
        Filter(flatten(condition).head, child)

      case Project(projectList, child) =>
        Project(flattenNamed(projectList), child)

      case CollectPlaceholder(projectList, child) =>
        CollectPlaceholder(flattenNamed(projectList), child)
    }
  }
}

/**
 * Insert lazy evaluates, and prepare for embedding lineage into plan.
 * REMARK:
 *  1. lazy evaluates are only necessary for attributes
 *    whose value can change and will be accessed later in the plan, including be output to users.
 *  2. An attribute may change because it is affected by a running value.
 *    A *running* value is flagged by bootstrap-counts.
 *    Uncertain tuples are always subsumed by tuples with bootstrap-counts,
 *    and thus do not need to be considered for now.
 *  3. we need to propagate all the attributes again
 *    because we changed the output attributes of operators.
 */
object IdentifyLazyEvaluates extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case q: SparkPlan =>
      val withNewLeaves: SparkPlan = q.transformExpressionsUp {
        case attr: Attribute => q.inputSet.find(_.exprId == attr.exprId).getOrElse(attr)
      }

      withNewLeaves match {
        case aggregate@BootstrapAggregate(false, groupings, aggrs, child) =>
          // An partial aggregate does not need to be converted to lazy evaluates,
          // even if its input has lazy evaluates,
          // because its output won't be reused
          val opId = OpId.newOpId
          val numShuffleParts = aggregate.sqlContext.conf.numShufflePartitions
          val keys = toNamed(groupings)

          val toNewAttr = mutable.HashMap[ExprId, Attribute]()
          val schema = aggrs.collect {
            case alias@TaggedAlias(Uncertain, _, _) =>
              // To distinguish the schema for lazy evals from the output
              val attribute = alias.toAlias.toAttribute.newInstance()
              toNewAttr(alias.exprId) = attribute
              attribute
            case alias@TaggedAlias(_: Bound, _, _) =>
              val attribute = alias.toAlias.toAttribute.newInstance()
              toNewAttr(alias.exprId) = attribute
              attribute
          }

          val newAggrs = aggrs.map {
            case alias@TaggedAlias(Uncertain, expr, name) =>
              TaggedAlias(
                LazyAggregate(opId, numShuffleParts, keys, schema, toNewAttr(alias.exprId)),
                expr, name)(alias.exprId, alias.qualifiers)
            case alias@TaggedAlias(bound: Bound, expr, name) =>
              TaggedAlias(
                LazyAggrBound(opId, numShuffleParts, keys, schema, toNewAttr(alias.exprId), bound),
                expr, name)(alias.exprId, alias.qualifiers)
            case expr => expr
          }
          BootstrapAggregate(partial = false, groupings, newAggrs, child)

        case project: Project =>
          project.transformExpressionsUp {
            case alias@TaggedAlias(Uncertain, child, name)
              if containsLazyEvaluates(alias.references) =>
              TaggedAlias(LazyAlias(child), child, name)(alias.exprId, alias.qualifiers)
          }

        case other => other
      }
  }
}

/**
 * Embed lineage into operators.
 */
object EmbedLineage extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case BootstrapAggregate(partial, grouping, aggrs, child) =>
      BootstrapAggregate(partial, grouping, aggrs ++ lineageNeedToPropagate(aggrs), child)

    case Project(projectList, child) =>
      Project(projectList ++ lineageNeedToPropagate(projectList), child)
  }

  // Filter out bootstrap-counts, it will be replaced with the bootstrap-counts in the current row.
  // This is a safe optimization to avoid propagating bootstrap-counts,
  // as the bootstrap-counts column is always monotonic in the AND lattice.
  def lineageNeedToPropagate(exprs: Seq[NamedExpression]): Seq[NamedExpression] =
    exprs.collect {
      case TaggedAlias(gen: GenerateLazyEvaluate, _, _) => gen.lineage
      case TaggedAttribute(lazyEval: LazyEvaluate, _, _, _, _) => lazyEval.lineage
    }.flatten.distinct.diff(exprs)
}

object ImplementSort extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case sort@ExternalSort(_, _, _: CollectPlaceholder) => sort

    // Hack: turn this off because we do not support "order by",
    //  but do need external sort for sort merge join.
    //  For now, sort merge join always do not grow on the cache side,
    //  and do not cache on the non-cache side,
    //  so this hack is correct
    //    case sort@ExternalSort(sortOrder, global, child) =>
    //      getFlag(child.output) match {
    //        case None => sort
    //        case Some(flag) =>
    //          val projectList = child.output.map {
    //            case a@TaggedAttribute(Flag, _, _, _, _) =>
    //              TaggedAlias(Flag, boolFalse, "_flag")(a.exprId, a.qualifiers)
    //            case a => a
    //          }
    //          ExternalSort(sortOrder, global, Project(projectList, child))
    //      }
  }
}

case class ImplementProject(controller: OnlineDataFrame) extends Rule[SparkPlan] {
  import org.apache.spark.sql.hive.online.Growth._

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case project@Project(projectList, child) if flagModified(projectList) =>
      val mode = getGrowthMode(project)
      require(mode.perPartition != Fixed,
        s"!!! Warning: unexpected growth mode $mode in ${project.simpleString}")
      OnlineProject(collectRefreshInfo(child.output),
        collectCacheInfo(child.output, project.output),
        mode.numPartitions == Fixed, projectList, child)(controller)
  }

  def flagModified(projectList: Seq[Expression]): Boolean = projectList.exists {
    case TaggedAlias(Flag, _, _) => true
    case _ => false
  }
}

/**
 * Remark:
 * 1. Take broadcast implementation out of aggregate,
 *  that can be implemented as a separate operator,
 *  and optimized by considering the plan as a whole (e.g., later broadcast join).
 * 2. Only aggregates that output lazy evaluates need to be broadcast.
 * 3. We have already assumed no partitioning of input relations.
 * 4. Caching can be categorized into 6 classes according to
 *  (1) whether the input tuples are uncertain, lazy or normal attributes
 *  (2) whether there is a flag column
 *
 *  uncertain --> no caching
 *  lazy --> per tuple caching
 *  normal --> partial result caching (TODO: currently use per tuple caching)
 *
 *  if flag exists, only those tuples with flag=true
 *  will be cached or merged into the cached partial result
 */
case class ImplementAggregate(slackParam: Double, controller: OnlineDataFrame)
  extends Rule[SparkPlan] {

  import org.apache.spark.sql.hive.online.Growth._

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case aggregate@BootstrapAggregate(partial, groupings, aggrs, child) =>
      val opId = aggrs.collectFirst {
        case TaggedAlias(tag: LazyAggregate, _, _) => tag.opId
      }.getOrElse(OpId.newOpId)
      val references = aggrs.flatMap(_.references)

      if (partial) {
        if (containsLazyEvaluates(references)) {
          require(getGrowthMode(aggregate).perPartition != Fixed,
            s"!!! Warning: unexpected growth mode ${getGrowthMode(aggregate)} " +
              s"in ${aggregate.simpleString}")
          AggregateWith2Inputs(getFlag(child.output), collectRefreshInfo(child.output),
            partial, groupings, aggrs, child)(controller, opId = opId)
        } else {
          aggregate
        }
      } else {
        require(getGrowthMode(aggregate).perPartition != Fixed,
          s"!!! Warning: unexpected growth mode ${getGrowthMode(aggregate)} " +
            s"in ${aggregate.simpleString}")
        AggregateWith2Inputs2Outputs(getFlag(child.output), collectRefreshInfo(child.output),
          collectLineageRelay(aggregate.output), collectIntegrityInfo(aggrs, slackParam),
          groupings, aggrs, child)(controller, -1 :: Nil, opId = opId)
      }
  }
}

/**
 * Remark:
 * 1. We use broadcast join for the side which is fixed or almost-fixed per partition.
 * 2. We should build both sides if neither side is fixed/almost-fixed per partition.
 *  (TODO: currently not implemented)
 */
case class ImplementJoin(controller: OnlineDataFrame) extends Rule[SparkPlan] {
  import org.apache.spark.sql.hive.online.Growth._

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case join@BroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right) =>
      val (buildMode, streamMode, build, stream) = buildSide match {
        case BuildLeft => (getGrowthMode(left), getGrowthMode(right), left, right)
        case BuildRight => (getGrowthMode(right), getGrowthMode(left), right, left)
      }
      (buildMode, streamMode) match {
        case (GrowthMode(_, Grow), GrowthMode(_, Grow)) => ???
        case (GrowthMode(Fixed, Fixed), GrowthMode(Fixed, Fixed)) => join
        case (GrowthMode(Fixed, Fixed), _) =>
          require(!containsLazyEvaluates(build.output),
            s"!!! Warning: incorrect growth mode in ${join.simpleString}")
          OTBroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right)(
            controller)
        case (GrowthMode(AlmostFixed, Fixed), _) =>
          MTBroadcastHashJoin(
            getFlag(left.output), getFlag(right.output),
            collectRefreshInfo(stream.output),
            collectRefreshInfo(build.output),
            leftKeys, rightKeys, buildSide, left, right)(
              controller)
        case (_, GrowthMode(Fixed, Fixed)) =>
          require(!containsLazyEvaluates(stream.output),
            s"!!! Warning: incorrect growth mode in ${join.simpleString}")
          OTBroadcastHashJoin(leftKeys, rightKeys, swap(buildSide), left, right)(
            controller)
        case (_, GrowthMode(AlmostFixed, Fixed)) =>
          MTBroadcastHashJoin(
            getFlag(left.output), getFlag(right.output),
            collectRefreshInfo(build.output),
            collectRefreshInfo(stream.output),
            leftKeys, rightKeys, swap(buildSide), left, right)(
              controller)
        case _ => ???
      }

    case join@ShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right) =>
      val (buildMode, streamMode, build, stream) = buildSide match {
        case BuildLeft =>
          (exchange(getGrowthMode(left)), exchange(getGrowthMode(right)), left, right)
        case BuildRight =>
          (exchange(getGrowthMode(right)), exchange(getGrowthMode(left)), right, left)
      }
      require(buildMode.numPartitions == Fixed && streamMode.numPartitions == Fixed,
        s"!!! Warning: unexpected growth mode ($buildMode, $streamMode) in ${join.simpleString}")
      (buildMode, streamMode) match {
        case (GrowthMode(Fixed, _), GrowthMode(Fixed, _)) => join
        case (GrowthMode(Fixed, _), _) =>
          require(!containsLazyEvaluates(build.output),
            s"!!! Warning: incorrect growth mode in ${join.simpleString}")
          OTShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right)(
            controller)
        case (_, GrowthMode(Fixed, _)) =>
          require(!containsLazyEvaluates(stream.output),
            s"!!! Warning: incorrect growth mode in ${join.simpleString}")
          OTShuffledHashJoin(leftKeys, rightKeys, swap(buildSide), left, right)(
            controller)
        case (GrowthMode(AlmostFixed, _), _) =>
          MTBroadcastHashJoin(
            getFlag(left.output), getFlag(right.output),
            collectRefreshInfo(stream.output), collectRefreshInfo(build.output),
            leftKeys, rightKeys, buildSide, left, right)(
              controller)
        case (_, GrowthMode(AlmostFixed, _)) =>
          MTBroadcastHashJoin(
            getFlag(left.output), getFlag(right.output),
            collectRefreshInfo(build.output), collectRefreshInfo(stream.output),
            leftKeys, rightKeys, swap(buildSide), left, right)(
              controller)
        case _ => ???
      }

    case join@SortMergeJoin(leftKeys, rightKeys, left, right) =>
      val leftMode = exchange(getGrowthMode(left))
      val rightMode = exchange(getGrowthMode(right))
      require(leftMode.numPartitions == Fixed && rightMode.numPartitions == Fixed,
        s"!!! Warning: unexpected growth mode ($leftMode, $rightMode) in ${join.simpleString}")
      (leftMode, rightMode) match {
        case (GrowthMode(Fixed, _), GrowthMode(Fixed, _)) => join
        case (GrowthMode(Fixed, _), _) =>
          require(!containsLazyEvaluates(left.output),
            s"!!! Warning: incorrect growth mode in ${join.simpleString}")
          OTSortMergeJoin(leftKeys, rightKeys, CacheLeft, left, right)(
            controller)
        case (GrowthMode(AlmostFixed, _), _) =>
          ???
          MTSortMergeJoin(getFlag(left.output), getFlag(right.output),
            collectRefreshInfo(left.output), collectRefreshInfo(right.output),
            leftKeys, rightKeys, CacheLeft, left, right)(
            controller)
        case (_, GrowthMode(Fixed, _)) =>
          require(!containsLazyEvaluates(right.output),
            s"!!! Warning: incorrect growth mode in ${join.simpleString}")
          OTSortMergeJoin(leftKeys, rightKeys, CacheRight, left, right)(
            controller)
        case (_, GrowthMode(AlmostFixed, _)) =>
          ???
          MTSortMergeJoin(getFlag(left.output), getFlag(right.output),
            collectRefreshInfo(left.output), collectRefreshInfo(right.output),
            leftKeys, rightKeys, CacheRight, left, right)(
            controller)
        case _ => ???
      }

    case join@LeftSemiJoinHash(leftKeys, rightKeys, left, right) =>
      val build = right
      val buildMode = exchange(getGrowthMode(right))
      val streamMode = exchange(getGrowthMode(left))
      require(buildMode.numPartitions == Fixed && streamMode.numPartitions == Fixed,
        s"!!! Warning: unexpected growth mode ($buildMode, $streamMode) in ${join.simpleString}")
      (buildMode, streamMode) match {
        case (GrowthMode(Fixed, _), GrowthMode(Fixed, _)) => join
        case (GrowthMode(Fixed, _), _) =>
          require(!containsLazyEvaluates(build.output),
            s"!!! Warning: incorrect growth mode in ${join.simpleString}")
          OTBLeftSemiHashJoin(leftKeys, rightKeys, left, right)(controller)
        case (GrowthMode(AlmostFixed, _), _) =>
          MTBLeftSemiHashJoin(leftKeys, rightKeys, left, right)(controller)
        case _ => ???
      }
  }

  def swap(buildSide: BuildSide): BuildSide = buildSide match {
    case BuildLeft => BuildRight
    case BuildRight => BuildLeft
  }
}

case class ImplementCollect(controller: OnlineDataFrame) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case CollectPlaceholder(projectList, child) =>
      Collect(getFlag(child.output), collectRefreshInfo(child.output),
        projectList, child)(controller)
  }
}

object CleanupAnalysisExpressions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case q: SparkPlan => q.transformExpressionsUp {
      case attr: TaggedAttribute => attr.toAttributeReference
      case alias: TaggedAlias => alias.toAlias
    }
  }
}

object OnlinePlannerUtil {
  import org.apache.spark.sql.hive.online.Growth._

  val boolTrue = Literal(true)
  val boolFalse = Literal(false)
  val byte0 = Literal(0.toByte)
  val byte1 = Literal(1.toByte)

  def getSeeds(attributes: Seq[Attribute]): Seq[Attribute] =
    attributes.collect { case seed@TaggedAttribute(_: Seed, _, _, _, _) => seed }

  /**
   * Get the first BootstrapCounts if any of this plan.
   */
  def getBtCnt(attributes: Seq[Attribute]): Option[TaggedAttribute] =
    attributes.collectFirst { case btCnt@TaggedAttribute(_: Bootstrap, _, _, _, _) => btCnt }

  /**
   * Get the first flag if any of this plan
   */
  def getFlag(attributes: Seq[Attribute]): Option[TaggedAttribute] =
    attributes.collectFirst { case flag@TaggedAttribute(Flag, _, _, _, _) => flag }

  def extractBounds(attributes: Seq[Attribute]): Map[ExprId, Seq[Attribute]] =
    attributes.collect { case a@TaggedAttribute(_: Bound, _, _, _, _) => a }
      .groupBy { case a@TaggedAttribute(b: Bound, _, _, _, _) => b.child }

  def explode(expr: Expression, map: Map[ExprId, Seq[Attribute]]): Seq[Expression] = expr match {
    case a: Attribute => ifnivetn (map.contains(a.exprId)) map(a.exprId) else Seq(a)
    case e => cartesian(e.children.map(c => explode(c, map))).map(e.withNewChildren)
  }

  private[this] def cartesian(exprs: Seq[Seq[Expression]]): Seq[Seq[Expression]] = exprs match {
    case Seq() => Seq(Nil)
    case _ => for (x <- exprs.head; y <- cartesian(exprs.tail)) yield x +: y
  }

  /**
   * Whether attributes contain uncertain ones.
   */
  def containsUncertain(attributes: Traversable[Attribute]) = attributes.exists {
    case TaggedAttribute(Uncertain, _, _, _, _) => true
    case _ => false
  }

  def widenTypes(left: Expression, right: Expression): (Expression, Expression) =
    if (left.dataType != right.dataType) {
      HiveTypeCoercion.findTightestCommonType(left.dataType, right.dataType)
        .map { widestType => (Cast(left, widestType), Cast(right, widestType))}
        .getOrElse((left, right))
    } else {
      (left, right)
    }

  val simplifyCast: PartialFunction[Expression, Expression] = {
    case expr@Cast(_: Literal, dataType) => Literal.create(expr.eval(EmptyRow), dataType)
    case Cast(e, dataType) if e.dataType == dataType => e
    case Cast(Cast(e, _), dataType) => if (e.dataType == dataType) e else Cast(e, dataType)
  }

  /**
   * Whether attributes contain lazy evalutes.
   */
  def containsLazyEvaluates(attributes: Traversable[Attribute]) = attributes.exists {
    case TaggedAttribute(_: LazyAttribute, _, _, _, _) => true
    case _ => false
  }

  def toNamed(exprs: Seq[Expression], name: String = "_key"): Seq[NamedExpression] =
    exprs.map {
      case named: NamedExpression => named
      case expr => Alias(expr, name)()
    }

  def collectRefreshInfo(output: Seq[Attribute]): RefreshInfo = {
    var hasLazy = false
    val dependencies = mutable.HashMap[OpId, (Seq[Attribute], Seq[NamedExpression], Int)]()

    val lazyEvals = output.collect {
      case TaggedAttribute(eval: LazyEvaluate, _, _, _, _) =>
        hasLazy = true
        dependencies ++= eval.dependencies
        eval.lazyEval.transformUp { case GetPlaceholder(attribute, _) => attribute}
      case attribute: TaggedAttribute => attribute.toAttributeReference
      case attribute => attribute
    }

    val inputSchema = output.map {
      case attribute: TaggedAttribute => attribute.toAttributeReference
      case attribute => attribute
    }

    RefreshInfo(if (hasLazy) lazyEvals else Seq(), dependencies.toSeq.sortBy(_._1.id), inputSchema)
  }

  def collectLineageRelay(output: Seq[Attribute]) = {
    val filterExpression = getFlag(output).get.toAttributeReference
    val broadcastExpressions = output.collect {
      case a@TaggedAttribute(_: LazyAttribute, _, _, _, _) => a.toAttributeReference
    }
    LineageRelay(filterExpression, broadcastExpressions)
  }

  def collectCacheInfo(input: Seq[Attribute], output: Seq[Attribute]): (Int, Int) = {
    def getFlagIndex(exprs: Seq[NamedExpression]) =
      exprs.indexWhere {
        case TaggedAttribute(Flag, _, _, _, _) => true
        case _ => false
      }
    (getFlagIndex(input), getFlagIndex(output))
  }

  def collectIntegrityInfo(
      exprs: Seq[NamedExpression], slackParam: Double): Option[IntegrityInfo] = {
    import BoundType._

    def cleanupAttrs(attrs: Seq[Attribute]): Seq[Attribute] = attrs.map {
      case a: TaggedAttribute => a.toAttributeReference
      case o => o
    }

    def stdev(xs: Seq[Attribute]): Expression = {
      val (sum, n) = widenTypes(xs.reduce(Add), Literal(xs.length))
      val mean = Divide(sum, n)
      val (sum2, _) = widenTypes(xs.map(a => Multiply(a, a)).reduce(Add), n)
      val mean2 = Divide(sum2, n)

      val dataType = xs.head.dataType
      Cast(Multiply(Literal(slackParam), Sqrt(Subtract(mean2, Multiply(mean, mean)))), dataType)
        .transformUp(simplifyCast)
    }

    exprs.zipWithIndex.collect {
      case (alias@TaggedAlias(LazyAggrBound(_, _, _, schema, attribute, bound), _, _), idx) =>
        val boundAttrs = cleanupAttrs(bound.boundAttributes)
        val tight = alias.toAlias.toAttribute
        val slack = stdev(boundAttrs)
        bound.boundType match {
          case Lower =>
            (Coalesce(GreaterThanOrEqual(tight, attribute) :: boolTrue :: Nil),
              (idx, MaxOf(Subtract(tight, slack), attribute)),
              schema)
          case Upper =>
            (Coalesce(LessThanOrEqual(tight, attribute) :: boolTrue :: Nil),
              (idx, MinOf(Add(tight, slack), attribute)),
              schema)
        }
    } match {
      case Seq() => None
      case all =>
        val (checks, updates, schemas) = all.unzip3
        val (updateIndexes, updateExpressions) = updates.unzip
        Some(IntegrityInfo(
          checks.reduce(And), updateIndexes.toArray, updateExpressions, schemas.head))
    }
  }

  def exchange(mode: GrowthMode): GrowthMode = mode match {
    case GrowthMode(p, n) => GrowthMode(Growth.max(p, n), Fixed)
  }

  def getGrowthMode(plan: SparkPlan): GrowthMode = {
    def fullAggregate(child: SparkPlan) = {
      exchange(getGrowthMode(child)) match {
        case GrowthMode(p, Fixed) => GrowthMode(Growth.min(p, AlmostFixed), Fixed)
      }
    }

    def broadcastJoin(buildSide: BuildSide, left: SparkPlan, right: SparkPlan) = {
      val (buildMode, streamMode) = buildSide match {
        case BuildLeft => (getGrowthMode(left), getGrowthMode(right))
        case BuildRight => (getGrowthMode(right), getGrowthMode(left))
      }
      (buildMode, streamMode) match {
        case (GrowthMode(bp, bn), GrowthMode(sp, sn)) =>
          GrowthMode(Seq(sp, bp, bn).reduce(Growth.max), sn)
      }
    }

    plan match {
      case _: LeafNode => GrowthMode(Fixed, Fixed)
      case sample: StreamedRelation =>
        require(getGrowthMode(sample.child) == GrowthMode(Fixed, Fixed),
          s"!!! Warning: unexpected growth mode ${getGrowthMode(sample.child)} " +
            s"in ${sample.simpleString}.")
        GrowthMode(Fixed, Grow)
      case aggregate: Aggregate =>
        if (aggregate.partial) getGrowthMode(aggregate.child) else fullAggregate(aggregate.child)
      case aggregate: BootstrapAggregate =>
        if (aggregate.partial) getGrowthMode(aggregate.child) else fullAggregate(aggregate.child)
      case aggregate: AggregateWith2Inputs2Outputs =>
        fullAggregate(aggregate.child)
      case aggregate: AggregateWith2Inputs =>
        getGrowthMode(aggregate.child)
      case _: ShuffledHashJoin | _: LeftSemiJoinHash | _: SortMergeJoin |
           _: OTShuffledHashJoin | _: OTSortMergeJoin | _: MTSortMergeJoin =>
        val modes = plan.children.map(getGrowthMode).map(exchange)
        GrowthMode(modes.map(_.perPartition).reduce(Growth.max), Fixed)
      case join: BroadcastHashJoin =>
        broadcastJoin(join.buildSide, join.left, join.right)
      case join: OTBroadcastHashJoin =>
        broadcastJoin(join.buildSide, join.left, join.right)
      case join: MTBroadcastHashJoin =>
        broadcastJoin(join.buildSide, join.left, join.right)
      case join: OTBLeftSemiHashJoin =>
        broadcastJoin(join.buildSide, join.left, join.right)
      case join: MTBLeftSemiHashJoin =>
        broadcastJoin(join.buildSide, join.left, join.right)
      case unary: UnaryNode => getGrowthMode(unary.child)
      case _ => ???
    }
  }
}

object Growth extends Enumeration {
  type Growth = Value
  val Fixed, AlmostFixed, Grow = Value

  def min(x: Growth, y: Growth) = if (x < y) x else y

  def max(x: Growth, y: Growth) = if (x < y) y else x
}

case class GrowthMode(perPartition: Growth.Growth, numPartitions: Growth.Growth) {
  require(numPartitions != Growth.AlmostFixed)
}
