package de.tuberlin.dima.pagerank;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class InitializeCoGroup extends CoGroupStub {

	PactDouble defaultRank = new PactDouble(0.15);
	PactString theString = new PactString();

	@Override
	public void coGroup(Iterator<PactRecord> records1,
			Iterator<PactRecord> records2, Collector<PactRecord> out) {
		List<PactRecord> list = new ArrayList<PactRecord>();

		boolean isSafed = false;
		while (records1.hasNext()) {
			PactRecord record = records1.next();

			if (!isSafed && !records2.hasNext()) {
				// page with no inLink need a PageRank
				record.getField(0, theString);
				PactRecord noInLinkPage = new PactRecord(theString, theString);
				noInLinkPage.setField(2, defaultRank);
				out.collect(noInLinkPage);
				isSafed = true;
			}

			// change from and to page
			record.getField(0, theString);
			record.setField(0, record.getField(1, PactString.class));
			record.setField(1, theString);

			// initialize default PageRank
			record.setField(2, defaultRank);
			list.add(record);
		}
		for (PactRecord r : list) {
			// toPage, fromPage, fromRank, fromOut
			r.setField(3, new PactInteger(list.size()));
			r.setNumFields(4);
			out.collect(r);
		}
	}
}
