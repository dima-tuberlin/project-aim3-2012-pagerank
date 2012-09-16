package de.tuberlin.dima.pagerank.danglingNodes;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class DanglingNodesCross extends CrossStub {

	@Override
	public void cross(PactRecord record1, PactRecord record2,
			Collector<PactRecord> out) {
		Double doub = record1.getField(1, PactDouble.class).getValue()
				/ record2.getField(0, PactInteger.class).getValue();
		record1.setField(1, new PactDouble(doub));
		out.collect(record1);
	}

}
