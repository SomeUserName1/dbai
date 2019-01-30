/*
 * @(#)RuleManager.java   1.0   May 10, 2014
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2016 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.optimizer.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import minibase.query.optimizer.OptimizerError;

/**
 * The rule manager manages all rules that are known to the optimizer and can be used to turn individual rules
 * on an off.
 * <p>
 * Minibase's query optimizer is based on the Cascades framework for query optimization and, additionally,
 * implements some of the improvements proposed by the Columbia database query optimizer.
 * <ul>
 * <li>Goetz Graefe: <strong>The Cascades Framework for Query Optimization</strong>. In
 * <em>IEEE Data(base) Engineering Bulletin</em>, 18(3), pp. 19-29, 1995.</li>
 * <li>Yongwen Xu: <strong>Efficiency in Columbia Database Query Optimizer</strong>,
 * <em>MSc Thesis, Portland State University</em>, 1998.</li>
 * </ul>
 * The Minibase query optimizer therefore descends from the EXODUS, Volcano, Cascades, and Columbia line of
 * query optimizers, which all use a rule-based, top-down approach to explore the space of possible query
 * execution plans, rather than a bottom-up approach based on dynamic programming.
 * </p>
 *
 * @author Michael Grossniklaus &lt;michael.grossniklaus@uni.kn&gt;
 * @version 1.0
 */
public final class RuleManager {

   /** Array of all rules known to the operator. */
   private final Rule[] rules;

   /** Bit vector to mark rules as active or inactive. */
   private final boolean[] active;

   /**
    * Constructor that creates a new rule manager instance by initializing the array of all rules and
    * the array of activated rules. Initially, all rules are activated.
    */
   public RuleManager() {
      this.rules = new Rule[RuleType.values().length];
      this.addRule(new EquiJoinCommute());
      this.addRule(new EquiJoinLeftToRight());
      this.addRule(new ProjectionThroughSelection());
      this.addRule(new GetTableToFileScan());
      this.addRule(new SelectionToFilter());
      this.addRule(new SelectionToIndexFilter());
      this.addRule(new ProjectionToTruncate());
      this.addRule(new DistinctToHashDuplicates());
      this.addRule(new EquiJoinToNestedLoopsJoin());
      this.addRule(new EquiJoinToMergeJoin());
      this.addRule(new EquiJoinToHashJoin());
      this.addRule(new EquiJoinToIndexNestedLoopsJoin());
      this.addRule(new EquiJoinToBitmapIndexJoin());
      this.addRule(new SortEnforcer());
      this.active = new boolean[this.rules.length];
      Arrays.fill(this.active, true);
   }

   /**
    * Returns the rule that corresponds to the given rule type.
    *
    * @param type
    *           rule type
    * @return optimizer rule
    */
   public Rule getRule(final RuleType type) {
      return this.rules[type.ordinal()];
   }

   /**
    * Returns a list of all currently activated rules.
    *
    * @return list of active rules.
    */
   public List<Rule> getActiveRules() {
      // TODO The list of active rules should be maintained and not generated on demand.
      final List<Rule> rules = new ArrayList<>();
      for (int i = 0; i < this.active.length; i++) {
         if (this.active[i] && this.rules[i] != null) {
            rules.add(this.rules[i]);
         }
      }
      return Collections.unmodifiableList(rules);
   }

   /**
    * Sets the activation status of the given rule.
    *
    * @param rule
    *           optimizer rule
    * @param active
    *           {@code true} if the rule should be used by the optimizer, {@code false} otherwise
    */
   public void setRuleStatus(final Rule rule, final boolean active) {
      // TODO Implement this method.
      throw new OptimizerError("Method not implemented: RuleManager#setRuleStatus(Rule, boolean).");
   }

   /**
    * Returns the total number of rules known to the optimizer.
    *
    * @return rule count
    */
   public int getRuleCount() {
      return this.rules.length;
   }

   /**
    * Adds the given rule to the rule set.
    *
    * @param rule
    *           optimizer rule.
    */
   private void addRule(final Rule rule) {
      this.rules[rule.getIndex()] = rule;
   }
}
