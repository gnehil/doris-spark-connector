// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.read.expression

import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{And, Not, Or, Predicate}
import org.apache.spark.sql.sources.{
  EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull,
  IsNull, LessThan, LessThanOrEqual, Not => SourceNot, Or => SourceOr, And => SourceAnd
}

/**
 * Converts V2 [[Predicate]]s back to V1 [[Filter]]s so that
 * [[org.apache.doris.spark.read.stats.SelectivityEstimator]] can work
 * uniformly across Spark 3.1 - 3.5.
 *
 * <p>Only the subset needed for selectivity estimation is converted;
 * anything unrecognised is dropped (returns [[None]]).
 */
private[spark] object V2ToV1FilterAdapter {

  def convert(predicates: Array[Predicate]): Array[Filter] = {
    if (predicates == null || predicates.isEmpty) return Array.empty
    predicates.flatMap(convertPredicate).toArray
  }

  private def convertPredicate(expr: Expression): Option[Filter] = expr match {
    case and: And =>
      for {
        l <- convertPredicate(and.left())
        r <- convertPredicate(and.right())
      } yield new SourceAnd(l, r)

    case or: Or =>
      for {
        l <- convertPredicate(or.left())
        r <- convertPredicate(or.right())
      } yield new SourceOr(l, r)

    case not: Not =>
      convertPredicate(not.child()).map(new SourceNot(_))

    case e: GeneralScalarExpression => convertScalar(e)

    case _ => None
  }

  private def convertScalar(e: GeneralScalarExpression): Option[Filter] = {
    val children = e.children()
    if (children == null || children.isEmpty) return None
    e.name() match {
      case "=" =>
        for { (attr, value) <- extractAttrValue(children) } yield EqualTo(attr, value)
      case ">" =>
        for { (attr, value) <- extractAttrValue(children) } yield GreaterThan(attr, value)
      case ">=" =>
        for { (attr, value) <- extractAttrValue(children) } yield GreaterThanOrEqual(attr, value)
      case "<" =>
        for { (attr, value) <- extractAttrValue(children) } yield LessThan(attr, value)
      case "<=" =>
        for { (attr, value) <- extractAttrValue(children) } yield LessThanOrEqual(attr, value)
      case "IN" =>
        if (children.length < 2) return None
        val attrOpt = extractAttr(children(0))
        val values = children.slice(1, children.length).flatMap(extractValue)
        attrOpt.filter(_ => values.nonEmpty).map(In(_, values))
      case "IS_NULL" =>
        extractAttr(children(0)).map(IsNull)
      case "IS_NOT_NULL" =>
        extractAttr(children(0)).map(IsNotNull)
      case _ => None
    }
  }

  private def extractAttrValue(children: Array[Expression]): Option[(String, Any)] = {
    for {
      attr <- extractAttr(children(0))
      value <- extractValue(children(1))
    } yield (attr, value)
  }

  private def extractAttr(e: Expression): Option[String] = e match {
    case nr: NamedReference => Some(nr.toString)
    case _ => None
  }

  private def extractValue(e: Expression): Option[Any] = e match {
    case lit: Literal[_] =>
      val v = lit.value()
      if (v == null) None else Some(v)
    case _ => None
  }
}
