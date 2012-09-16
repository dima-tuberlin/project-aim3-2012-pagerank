package de.tuberlin.dima.pagerank;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

public class PageRankMatcher extends MatchStub {


	@Override
	public void match(PactRecord value1, PactRecord value2,
			Collector<PactRecord> out) throws Exception {
		value1.setField(2, value2.getField(1, PactDouble.class));
		out.collect(value1);
	}

}
