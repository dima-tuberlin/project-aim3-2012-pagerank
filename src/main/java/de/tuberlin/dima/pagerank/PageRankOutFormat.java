package de.tuberlin.dima.pagerank;

import java.io.IOException;

import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Writes <tt>PactRecord</tt> containing an string (word) and an integer (count)
 * to a file. The output format is: "&lt;word&gt; &lt;count&gt;\n"
 */
public class PageRankOutFormat extends FileOutputFormat {
	private final StringBuilder buffer = new StringBuilder();

	@Override
	public void writeRecord(PactRecord record) throws IOException {
		this.buffer.setLength(0);
		this.buffer.append(record.getField(0, PactString.class).toString());
		this.buffer.append("\t");
		this.buffer.append(record.getField(1, PactDouble.class).getValue());
		this.buffer.append('\n');

		byte[] bytes = this.buffer.toString().getBytes();
		this.stream.write(bytes);
	}
}