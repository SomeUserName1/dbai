/*
 * @(#)EquiJoinCommute.java   1.0   Jan 30, 2017
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;


import java.util.List;

import minibase.query.optimizer.Expression;
import minibase.query.optimizer.PhysicalProperties;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.schema.ColumnReference;

public class EquiJoinCommute extends AbstractRule {

   public EquiJoinCommute() {
   }

   @Override
   public Expression nextSubstitute(final Expression before, final PhysicalProperties requiredProperties) {
   }
}
