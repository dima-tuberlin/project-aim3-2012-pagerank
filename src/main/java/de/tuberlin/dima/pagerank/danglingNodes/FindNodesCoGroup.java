package de.tuberlin.dima.pagerank.danglingNodes;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class FindNodesCoGroup extends CoGroupStub {

	@Override
	public void coGroup(Iterator<PactRecord> records1,
			Iterator<PactRecord> records2, Collector<PactRecord> out) {

		if (records1.hasNext() && !records2.hasNext()) {
			PactRecord record = records1.next();
			// safe the key as the value
			record.setField(1, record.getField(0, PactString.class));
			out.collect(record);
		}
	}
}
