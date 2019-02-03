/*
 * @(#)HashJoinTest.java   1.0   Jan 30, 2017
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2017 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.operators.physical;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import minibase.query.optimizer.Cost;
import minibase.query.optimizer.CostModel;
import minibase.query.optimizer.Expression;
import minibase.query.optimizer.LogicalCollectionProperties;
import minibase.query.optimizer.LogicalProperties;
import minibase.query.optimizer.OptimizerBaseTest;
import minibase.query.optimizer.operators.logical.EquiJoin;
import minibase.query.optimizer.operators.logical.GetTable;
import minibase.storage.file.DiskManager;

public class HashJoinTest extends OptimizerBaseTest {
   
   @Test
   public void testCost() {
      final Expression q = this.buildSimpleSRBJoinQuery();
      final Expression l = q.getInput(0);
      final EquiJoin eqJoin = (EquiJoin) l.getOperator();
      final Expression sailors = l.getInput(0);
      final Expression reserves = l.getInput(1);
      final LogicalProperties sProps = ((GetTable) (sailors.getOperator())).getLogicalProperties();
      final LogicalProperties rProps = ((GetTable) (reserves.getOperator())).getLogicalProperties();
      final LogicalProperties resultProps = eqJoin.getLogicalProperties(sProps, rProps);
      final HashJoin h = new HashJoin(eqJoin.getLeftColumns(), eqJoin.getRightColumns());
      final Cost costs = h.getLocalCost(resultProps, sProps, rProps);

      final double leftNumPages =
            Math.ceil(sProps.getCardinality() * ((LogicalCollectionProperties) sProps).getSchema().getLength()
                  / DiskManager.PAGE_SIZE);
      final double rightNumPages =
            Math.ceil(rProps.getCardinality() * ((LogicalCollectionProperties) rProps).getSchema().getLength()
                  / DiskManager.PAGE_SIZE);
      assertTrue("I/O costs are too low, every record has to be both written and read at least once.",
            costs.getIO() >= 1.9 * (leftNumPages + rightNumPages) * CostModel.IO_SEQ.getCost());
      assertTrue("I/O costs are too high, you estimate at least 4 I/O operations per record.",
            costs.getIO() < 4 * (leftNumPages + rightNumPages) * CostModel.IO_SEQ.getCost());
      assertTrue("CPU costs are too low, every record has to be hashed at least once.",
            costs.getCPU() >= 0.9 * (sProps.getCardinality() + rProps.getCardinality())
               * CostModel.HASH_COST.getCost());
   }
}
