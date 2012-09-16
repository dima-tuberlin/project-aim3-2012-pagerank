/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package de.tuberlin.dima.pagerank;

import de.tuberlin.dima.pagerank.danglingNodes.CalculateDanglingNodesCross;
import de.tuberlin.dima.pagerank.danglingNodes.CountNodesReducer;
import de.tuberlin.dima.pagerank.danglingNodes.DanglingNodesCross;
import de.tuberlin.dima.pagerank.danglingNodes.FindNodesCoGroup;
import de.tuberlin.dima.pagerank.danglingNodes.NodesRankMatcher;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * @author Stefan Medack
 */
public class Main implements PlanAssembler, PlanAssemblerDescription {

	public int numberOfCalculations = 1;

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
//		numberOfCalculations = (args.length > 1 ? Integer.parseInt(args[1]) : 1);
		String dataInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		FileDataSource source = new FileDataSource(PageRankLinksInFormat.class,
				dataInput, "Parse Links");

		CoGroupContract initCoG = new CoGroupContract(InitializeCoGroup.class,
				new Class[] { PactString.class }, new int[] { 0 },
				new int[] { 1 }, "Init list");
		initCoG.setFirstInput(source);
		initCoG.setSecondInput(source);

		ReduceContract pageRankRed = new ReduceContract(PageRankReducer.class,
				PactString.class, 0, initCoG, "Compute PageRank #1");

		// counting the nodes
		MatchContract countNodesMatch = new MatchContract(
				NodesRankMatcher.class, new Class[] { PactString.class },
				new int[] { 0 }, new int[] { 0 }, "Count Nodes");
		countNodesMatch.setFirstInput(pageRankRed);
		countNodesMatch.setSecondInput(pageRankRed);

		ReduceContract countNodesReduce = new ReduceContract(
				CountNodesReducer.class, PactString.class, 0, countNodesMatch);

		CrossContract dangling = calcDanglingNodes(source, pageRankRed,
				countNodesReduce);

		for (int i = 1; i < numberOfCalculations; i++) {

			// TODO For dynamic source, reread first input
			MatchContract match = new MatchContract(PageRankMatcher.class,
					new Class[] { PactString.class }, new int[] { 1 },
					new int[] { 0 }, "Update PageRanks #" + i);
			match.setFirstInput(initCoG);
			match.setSecondInput(dangling);

			pageRankRed = new ReduceContract(PageRankReducer.class,
					PactString.class, 0, match, "Compute PageRank #" + (i + 1));

			dangling = calcDanglingNodes(source, pageRankRed, countNodesReduce);
		}

		FileDataSink out = new FileDataSink(PageRankOutFormat.class, output,
				pageRankRed, "PageRank iteration complete");

		Plan plan = new Plan(out, "PageRank");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [numOfIterations] [input] [output]";
	}

	@SuppressWarnings("unchecked")
	private CrossContract calcDanglingNodes(FileDataSource source,
			ReduceContract pageRankRed, ReduceContract countNodesReduce) {

		// Finding dangling nodes
		CoGroupContract danglingNodesCoG = new CoGroupContract(
				FindNodesCoGroup.class, new Class[] { PactString.class },
				new int[] { 1 }, new int[] { 0 }, "Find DN");
		danglingNodesCoG.setFirstInput(source);
		danglingNodesCoG.setSecondInput(source);

		// Mapping dangling nodes with calculated PageRanks
		MatchContract danglingMatch = new MatchContract(NodesRankMatcher.class,
				new Class[] { PactString.class }, new int[] { 0 },
				new int[] { 0 }, "map DN and PR");
		danglingMatch.setFirstInput(danglingNodesCoG);
		danglingMatch.setSecondInput(pageRankRed);

		// Count nodes (every node is once in the pageRank-result)

		// Divide PageRanks by total node count
		CrossContract danglingNodesCross = new CrossContract(
				DanglingNodesCross.class, "Divide DN ranks");
		danglingNodesCross.setFirstInput(danglingMatch);
		danglingNodesCross.setSecondInput(countNodesReduce);

		// Sum to existing pageRank results
		CrossContract calcDanglingCross = new CrossContract(
				CalculateDanglingNodesCross.class, "Add DN ranks");
		calcDanglingCross.setFirstInput(pageRankRed);
		calcDanglingCross.setSecondInput(danglingNodesCross);

		return calcDanglingCross;
	}

}
