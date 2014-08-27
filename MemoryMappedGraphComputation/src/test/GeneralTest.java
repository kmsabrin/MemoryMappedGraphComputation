package test;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import edu.gatech.memory_mapping_alg.MemoryMappingGraph;
import edu.gatech.memory_mapping_alg.Timer;

public class GeneralTest {
	Timer testTimer;
	MemoryMappingGraph mmg;

	@Before
	public void setUp() throws Exception {
		testTimer = new Timer();

//		GraphConstructor.makeIntegerGraph("C://Users//Lupu//Downloads//LiveJournal//LiveJournal.txt", true, true);
//		GraphConstructor.makeIntegerGraph("D:/bigdata/Twitter/Twitter.txt", true, true);
//		GraphConstructor.makeIntegerGraph("test2.txt", true, false);
		
		testTimer.showTimeElapsed("convert to bin");

		mmg = new MemoryMappingGraph();

		mmg = mmg.loadExistingBinary(new File("C://Users//Lupu//Downloads//LiveJournal//LiveJournal.txt.bin"), 4847570 + 1);
//		mmg = mmg.loadExistingBinary(new File("D:/bigdata/Twitter/Twitter.txt.bin"),41652230, true);
//		mmg = mmg.loadExistingBinary(new File("test2.txt.bin"), 6);

		testTimer.showTimeElapsed("load bin");
	}

	//@Test
	public void connectedComponentTest() {
		testTimer.reset();
		
//		mmg.getAllConnectedComponent();
//		mmg.getAllConnectedComponentUnsafe();
		
		testTimer.showTimeElapsed("Outside Time of Finding Connected Component");
		
		System.out.println("community num: " + mmg.countCCNum());
	}

	//@Test
	public void pageRankTest() {
		int prIterationNum = 5;
		testTimer.reset();

		mmg.resetPageRank();
		mmg.pageRank(prIterationNum);
		
		testTimer.showTimeElapsed("Running "+prIterationNum+" iterations of pageRank");

		System.out.println("pr: " + mmg.topPageRank(20) + "sum: " + mmg.pageRankSum());
	}
	
//	@Test
	public void randomWalkWithRestartTest() {
		int iterationNum = 10;
		testTimer.reset();

		int sourceNode = 1000;
		mmg.randomWalkWithRestart(sourceNode, iterationNum);
		
		testTimer.showTimeElapsed("Running "+iterationNum+" iterations of randomWalkWithRestart");
		
		int k = 20;
		System.out.println("closest "+k+" nodes : \n" + mmg.topCloseNode(k));
	}
	
	
//	@Test
	public void oneStepNeighborTest() {
		testTimer.reset();
		
		int sourceNode = 10000;
		Set<Integer> neighborSet = mmg.getOneStepNeighbor(sourceNode);
		
		testTimer.showTimeElapsed("Time of one step neighbor");
		
		System.out.println("One Step Neighbor Size of "+sourceNode+" is "+neighborSet.size());
	}
	
//	@Test
	public void oneStepNeighborIndexedTest() {
		testTimer.reset();
		
		int sourceNode = 10000;
		List<Integer> neighborList = mmg.getOneStepNeighborFromIndex(sourceNode, 
				"C://Users//Lupu//Downloads//LiveJournal//LiveJournal.txt.bin",
				"C://Users//Lupu//Downloads//LiveJournal//LiveJournal.index.bin");
		
		testTimer.showTimeElapsed("Time of one step neighbor");
		
		System.out.println("One Step Neighbor Size of "+sourceNode+" is "+neighborList.size());
	}
	
	@Test
	public void twoStepNeighborTest() {
		testTimer.reset();
		
		int sourceNode = 100000;
		Set<Integer> neighborSet = mmg.getTwoStepNeighbor(sourceNode);
		
		testTimer.showTimeElapsed("Time of two step neighbor");
		
		System.out.println("Two Step Neighbor Size of "+sourceNode+" is "+neighborSet.size());
	}
	
	@Test
	public void twoStepNeighborIndexedTest() {
		testTimer.reset();
		
		int sourceNode = 100000;
		Set<Integer> neighborSet = mmg.getTwoStepNeighborFromIndex(sourceNode, 
				"C://Users//Lupu//Downloads//LiveJournal//LiveJournal.txt.bin",
				"C://Users//Lupu//Downloads//LiveJournal//LiveJournal.index.bin");
		testTimer.showTimeElapsed("Time of two step neighbor");
		
		System.out.println("Two Step Neighbor Size of "+sourceNode+" is "+neighborSet.size());
	}
}
