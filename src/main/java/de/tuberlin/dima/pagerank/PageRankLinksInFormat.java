package de.tuberlin.dima.pagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class PageRankLinksInFormat extends TextInputFormat {

	private final PactString key = new PactString();
	private final PactString value = new PactString();

	@Override
	public boolean readRecord(PactRecord record, byte[] line, int numBytes) {
		int startPos;
		for (startPos = 0; startPos < numBytes; startPos++) {
			if (line[startPos] == ',') {
				break;
			}
		}
		if (numBytes <= startPos) {
			return false;
		}
		this.key.setValueAscii(line, 0, startPos++);
		this.value.setValueAscii(line, startPos, numBytes - startPos);
		record.setField(0, this.key);
		record.setField(1, value);
		return true;
	}
}