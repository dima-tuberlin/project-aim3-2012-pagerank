package de.tuberlin.dima.pagerank;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Sums up the counts for a certain given key. The counts are assumed to be at
 * position <code>1</code> in the record. The other fields are not modified.
 */
public class PageRankReducer extends ReduceStub {
	private static final float dampingFactor = 0.85F;
	PactRecord element = null;
	PactDouble doub = new PactDouble();

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		double sumShareOtherPageRanks = 0;

		while (records.hasNext()) {

			element = records.next();
			if (element.getNumFields() < 3) {
				// stop parsing for this element
				continue;
			}

			element.getField(2, doub);
			if(doub==null){
				continue;
			}
			double pageRank = doub.getValue();
			
			int outLinkCount = 0;
			if (element.getNumFields() > 3) {
				outLinkCount = element.getField(3, PactInteger.class)
						.getValue();
				sumShareOtherPageRanks += (pageRank / outLinkCount);
			}
		}

		double newRank = dampingFactor * sumShareOtherPageRanks
				+ (1 - dampingFactor);
		doub.setValue(newRank);

		element.setField(1, doub);
		element.setNumFields(2);
		out.collect(element);
	}
}