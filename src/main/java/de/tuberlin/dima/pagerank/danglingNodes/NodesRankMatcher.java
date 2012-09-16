package de.tuberlin.dima.pagerank.danglingNodes;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class NodesRankMatcher extends MatchStub {

	@Override
	public void match(PactRecord value1, PactRecord value2,
			Collector<PactRecord> out) throws Exception {
		out.collect(new PactRecord(new PactString("*"), value2.getField(1,
				PactDouble.class)));
	}

}
