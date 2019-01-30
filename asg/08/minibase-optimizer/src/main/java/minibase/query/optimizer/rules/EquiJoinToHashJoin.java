/*
 * @(#)EquiJoinToHashJoin.java   1.0   Jun 15, 20142
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.SearchContext;
import minibase.query.optimizer.operators.Operator;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.physical.HashJoin;

public class EquiJoinToHashJoin extends AbstractRule {

   public EquiJoinToHashJoin() {
   }

   @Override
   public Promise getPromise(final Operator operator, final SearchContext context) {
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
   }
}
