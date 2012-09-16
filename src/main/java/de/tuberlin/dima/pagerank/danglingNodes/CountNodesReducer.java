package de.tuberlin.dima.pagerank.danglingNodes;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Sums up the counts for a certain given key. The counts are assumed to be at
 * position <code>1</code> in the record. The other fields are not modified.
 */
public class CountNodesReducer extends ReduceStub {
	PactInteger theInteger = new PactInteger();

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		int sumNodes = 0;

		while (records.hasNext()) {

			records.next();
			sumNodes += 1;
		}

		theInteger.setValue(sumNodes);
		out.collect(new PactRecord(theInteger));
	}
}