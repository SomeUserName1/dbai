/*
 * @(#)RandomTuples.java   1.0   Jan 10, 2019
 *
 * Copyright (c) 1996-1997 University of Wisconsin.
 * Copyright (c) 2006 Purdue University.
 * Copyright (c) 2013-2018 University of Konstanz.
 *
 * This software is the proprietary information of the above-mentioned institutions.
 * Use is subject to license terms. Please refer to the included copyright notice.
 */
package minibase.query.evaluator;

import java.util.ArrayList;
import java.util.Random;

import minibase.query.schema.Schema;

/**
 * @author fabian.klopfer@uni-konstanz.de, simon.suckut@uni-konstanz.de
 * @version 1.0
 */
public class RandomTuples implements Operator, TupleIterator {

    /**
     * Schema to determine length of each tuple.
     */
    private final Schema schema;

    /**
     * Random object to generate tuples with.
     */
    private final long randomSeed;

    /**
     * Probablity to return a duplicate.
     */
    private final double duplicateProbability;

    /**
     * How many tuples to generate.
     */
    private final int maxTuples;

    /**
     * Random object to generate tuples with.
     */
    private Random random;

    /**
     * last returned tuple.
     */
    private ArrayList<byte[]> alreadyReturned;

    /**
     * How many tuples have been returned. (Inclusive duplicates).
     */
    private int returnedTuples;

    /**
     * Constructor.
     *
     * @param schema               schema to get the length of each tuple from
     * @param randomSeed           random with user defined seed.
     * @param duplicateProbability to return a tuple which was already returned
     * @param maxTuples            How many tuples to generate
     */
    RandomTuples(final Schema schema, final long randomSeed, final double duplicateProbability,
                 final int maxTuples) {
        this.schema = schema;
        this.randomSeed = randomSeed;
        this.duplicateProbability = duplicateProbability;
        this.alreadyReturned = new ArrayList<>();
        this.maxTuples = maxTuples;

        this.reset();
    }

    @Override
    public TupleIterator open() {
        return new RandomTuples(this.schema, this.randomSeed, this.duplicateProbability, this.maxTuples);
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public boolean hasNext() {
        return this.returnedTuples < this.maxTuples;
    }

    @Override
    public byte[] next() {
        final byte[] newTuple = new byte[this.schema.getLength()];
        this.returnedTuples++;
        if (this.random.nextDouble() >= this.duplicateProbability) {
            this.random.nextBytes(newTuple);
            this.alreadyReturned.add(newTuple);
            return newTuple;
        } else {
            return this.alreadyReturned.get(this.random.nextInt(this.alreadyReturned.size()));
        }
    }

    @Override
    public void reset() {
        this.returnedTuples = 0;
        this.random = new Random(this.randomSeed);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Random generator can not be closed");
    }
}
