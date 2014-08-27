package edu.gatech.memory_mapping_alg;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryMappingGraph {
	public static final int UNIT_SIZE = (int) 4L;
	private static final int DEFAULT_BLOCK_NUM = 50;
	private static final int THREAD_NUM = 4;
	private RandomAccessFile raf;
	private long fileSizeInByte;
	private long edgeNum;
	private long blockSize;
	private int blockNum;
	private int nodeNum;
	private boolean directed = false;
	private int[] ccId;
	// 1/outDegree
	private float[] oneOverOutDegree;
	private float[] pr;
	private final static float RESISTANCE = .85f;
	private float initialPRValue;
	private MappedByteBuffer[] byteBuffer;
	private UnsafeBigArray unsafeCcId;
	private float[] rwr;
	private float[] oneOverInDegree;
	
	public MemoryMappingGraph loadExistingBinary(File binFile, int nodeNum, boolean directed) {
		this.directed = directed;
		MemoryMappingGraph ret = loadExistingBinary(binFile, nodeNum);
		ret.countOutDegree();
		return ret;
	}

	private void countOutDegree() {
		oneOverOutDegree = new float[nodeNum];
		// ---------------MultiThreading counting-----------
		for (MappedByteBuffer block : byteBuffer) {
			block.position(0);
			while (block.position() < block.limit()) {
				int source = block.getInt();
				// skip the target nod
				block.getInt();
				oneOverOutDegree[source]++;
				// if (source < nodeNum) {
				// System.out.println("in bound: " + source+ " " + target);
				// } else {
				// System.out.println("Out of bound: " + source+ " " + target);
				// }
			}
		}
//		System.out.println("Done Counting outDegree");

		for (int i = 0; i < nodeNum; i++) {
			if (oneOverOutDegree[i] != 0) {
				oneOverOutDegree[i] = 1f / oneOverOutDegree[i];
			}
		}

//		System.out.println("Done Calculating oneOverOutDegree");
	}

	private void countInDegree() {
		oneOverInDegree = new float[nodeNum];
		
		for (MappedByteBuffer block : byteBuffer) {
			block.position(0);
			while (block.position() < block.limit()) {
				int source = block.getInt();
				int target = block.getInt();
				oneOverInDegree[target]++;
			}
		}
		System.out.println("Done Counting inDegree");

		for (int i = 0; i < nodeNum; i++) {
			if (oneOverInDegree[i] != 0) {
				oneOverInDegree[i] = 1f / oneOverInDegree[i];
			}
		}
		System.out.println("Done Calculating oneOverInDegree");
	}
	
	public MemoryMappingGraph loadExistingBinary(File binFile, int nodeNum) {
		// set initial value
		// assuming continuous ID
		this.nodeNum = nodeNum;
		initialPRValue = 1f / (float) nodeNum;

		// -----load bin file-------
		try {
			this.raf = new RandomAccessFile(binFile, "r");
			this.fileSizeInByte = raf.length();
			this.edgeNum = fileSizeInByte / 2 / UNIT_SIZE;
			// how many edges (number pairs) are going to be in each shard
			long edgeNumInEachShard;
			if (edgeNum < DEFAULT_BLOCK_NUM) {
				edgeNumInEachShard = 1;
				blockNum = (int) edgeNum;
			} else {
				edgeNumInEachShard = edgeNum / DEFAULT_BLOCK_NUM;
				if (edgeNum % DEFAULT_BLOCK_NUM != 0) {
					this.blockNum = DEFAULT_BLOCK_NUM + 1;
				} else {
					this.blockNum = DEFAULT_BLOCK_NUM;
				}
			}
			this.blockSize = edgeNumInEachShard * 2 * UNIT_SIZE;

			this.byteBuffer = new MappedByteBuffer[blockNum];
			for (int i = 0; i < blockNum - 1; i++) {
				byteBuffer[i] = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, i * blockSize, blockSize);
			}

			// deal with the last shard
			long edgeNumInLastShard = (edgeNum - edgeNumInEachShard * (blockNum - 1));
			long lastShardSizeInByte = edgeNumInLastShard * 2 * UNIT_SIZE;
			byteBuffer[blockNum - 1] = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, (blockNum - 1) * blockSize, lastShardSizeInByte);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return this;
	}

	/**
	 * reset all PageRank value
	 */
	public void resetPageRank() {
		// initial value of Pagerank
		pr = new float[nodeNum];
		Arrays.fill(pr, initialPRValue);
	}

	/**
	 * do one iteration of PageRank
	 */
	public void pageRank(int iterationNum) {

		// //----------Single Thread------------------
		//
		// float[] nextPR = new float[nodeNum];
		// float oneMinusResistance = (1 - RESISTANCE);
		// for (int i = 0; i < nodeNum; i++) {
		// nextPR[i] = initialPRValue * oneMinusResistance;
		// }
		//
		// if (!directed) {
		// for (MappedByteBuffer block : byteBuffer) {
		// block.position(0);
		// while (block.position() < block.limit()) {
		// int source = block.getInt();
		// int target = block.getInt();
		// nextPR[target] += RESISTANCE * pr[source] * oneOverOutDegree[source];
		// nextPR[source] += RESISTANCE * pr[target] * oneOverOutDegree[target];
		// }
		// }
		// } else {
		//
		// for (MappedByteBuffer block : byteBuffer) {
		// block.position(0);
		// while (block.position() < block.limit()) {
		// int source = block.getInt();
		// int target = block.getInt();
		// nextPR[target] += RESISTANCE * pr[source] * oneOverOutDegree[source];
		// }
		// }
		// }
		// //-----------done single thread------------

		// ----------Multi-thread------------
		final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);
		final float oneMinusResistance = (1 - RESISTANCE);
		float[] nextPR;
		CompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(threadPool);

		for (int i = 0; i < iterationNum; i++) {
			nextPR = new float[nodeNum];
			Arrays.fill(nextPR,initialPRValue * oneMinusResistance);
			
			if (!directed) {
				for (MappedByteBuffer block : byteBuffer) {
					final Callable<Boolean> thread = new PageRankThread(block, nextPR, false);
					ecs.submit(thread);
				}

			} else {

				for (MappedByteBuffer block : byteBuffer) {
					final Callable<Boolean> thread = new PageRankThread(block, nextPR, true);
					ecs.submit(thread);
				}

			}
			
			// wait till all threads finish
			try {
				for (int j = 0; j < blockNum; j++) {
					ecs.take();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// ----------done Multi-thread------------

			pr = nextPR;
		}
	}

	private class PageRankThread implements Callable<Boolean> {
		MappedByteBuffer block;
		float[] nextPR;
		boolean directed;

		public PageRankThread(MappedByteBuffer block, float[] nextPR, boolean directed) {
			this.block = block;
			this.nextPR = nextPR;
			this.directed = directed;
		}

		@Override
		public Boolean call() throws Exception {
			block.position(0);
			int source, target;
			if (directed) {
				while (block.position() < block.limit()) {
					source = block.getInt();
					nextPR[block.getInt()] += RESISTANCE * pr[source] * oneOverOutDegree[source];
				}
			} else {
				while (block.position() < block.limit()) {
					source = block.getInt();
					target = block.getInt();
					nextPR[target] += RESISTANCE * pr[source] * oneOverOutDegree[source];
					nextPR[source] += RESISTANCE * pr[target] * oneOverOutDegree[target];
				}

			}
			return true;
		}
	}

	/**
	 * @param k
	 * @return the top k page ranking;
	 */
	public String topPageRank(int k) {
		PriorityQueue<Double> topK = new PriorityQueue<Double>();
		for (float prValue : pr) {
			if (topK.size() < k) {
				topK.add((double) prValue);
			} else {
				if (prValue > topK.peek()) {
					topK.poll();
					topK.add((double) prValue);
				}
			}
		}

		String ret = "";
		while (!topK.isEmpty()) {
			ret = (topK.poll() + "\n") + ret;
		}
		return ret;
	}

	/**
	 * @return the result of pagerank
	 */
	public String pageRankResult() {
		String ret = "";
		for (float i : pr) {
			ret += (i + ", ");
		}

		return ret;
	}

	public float pageRankSum() {
		float ret = 0;
		for (float i : pr) {
			ret += i;
		}
		return ret;
	}

	// return number of connected component
	public void getAllConnectedComponent() {
		ccId = new int[nodeNum];
		for (int i = 0; i < ccId.length; i++) {
			ccId[i] = i;
		}
		AtomicBoolean converged = new AtomicBoolean(false);
		int iteration = 0;
		System.out.println("blockNum: " + blockNum);
		final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);
		CompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(threadPool);
		// keep scanning till converged
		Timer timer = new Timer();
		while (!converged.get()) {
			System.out.println("Iteration " + iteration);
			converged.set(true);
			// ---------------MultiThreading-----------
			for (MappedByteBuffer block : byteBuffer) {
				final Callable<Boolean> thread = new BlockIteratingThread(block, ccId, converged);
				ecs.submit(thread);
			}
			// wait till all threads finish
			try {
				for (int i = 0; i < blockNum; i++) {
					ecs.take();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// for (int i : ccId) {
			// System.out.print(i + " ");
			// }
			// System.out.println();
			iteration++;
		}
		threadPool.shutdown();
		timer.showTimeElapsed("Found all CCs");
		System.out.printf("Iteration #: %d%n", iteration);
	}

	private class BlockIteratingThread implements Callable<Boolean> {
		MappedByteBuffer block;
		AtomicBoolean converged;
		int[] ccId;

		public BlockIteratingThread(MappedByteBuffer block, int[] ccId, AtomicBoolean converged) {
			this.block = block;
			this.converged = converged;
			this.ccId = ccId;
		}

		@Override
		public Boolean call() throws Exception {
			boolean blockConverged = false;
			while (!blockConverged) {
				blockConverged = true;
				block.position(0);
				while (block.position() < block.limit()) {
					int source = block.getInt();
					int target = block.getInt();
					// System.out.println("edge: "+source+" "+target);
					// no indexOutOfBound Exception thrown
					if (ccId[source] != ccId[target]) {
						blockConverged = false;
						if (ccId[source] > ccId[target]) {
							ccId[source] = ccId[target];
						} else {
							ccId[target] = ccId[source];
						}
					}
				}
				if (blockConverged == false) {
					converged.set(false);
				}
			}
			return converged.get();
		}

	}

	public int countCCNum() {
		 Set<Integer> ccSet = new HashSet<>();
		 int ccNum = 0;
		 for (int i = 0; i < ccId.length; i++) {
		 // System.out.print(ccId[i]);
		 if (!ccSet.contains(ccId[i])) {
		 ccNum++;
		 ccSet.add(ccId[i]);
		 }
		 // System.out.println(i+" "+ccId[i]);
		 }
		 System.out.println("total Node: " + ccId.length);
		 return ccNum;
		
//		// TEMP ALGORITHM FOR TWITTER GRAPH		
//		for (int i = 0; i < ccId.length; i++) {
//			if (ccId[i] != 0) {
//				return -1;
//			}
//		}
//		return 0;
//		// DONE WITH TEMP ALG

	}
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	// author: kaeser
	public float[] randomWalkWithRestart(int sourceNode, int iterationNum) {
		final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);
		CompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(threadPool);
		float[] nextRWR;
		
		countOutDegree();
		
		rwr = new float[nodeNum];
		rwr[sourceNode] = 1;
		
		for (int i = 0; i < iterationNum; i++) {
			nextRWR = new float[nodeNum];
			
			for (MappedByteBuffer block : byteBuffer) {
				final Callable<Boolean> thread = new RandomWalkWithRestartThread(block, nextRWR);
				ecs.submit(thread);
			}

			try {
				for (int j = 0; j < blockNum; j++) {
					ecs.take();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			nextRWR[sourceNode] += RESISTANCE;
			rwr = nextRWR;
		}
		
		threadPool.shutdown();
		return rwr;
	}

	// author: kaeser
	private class RandomWalkWithRestartThread implements Callable<Boolean> {
		MappedByteBuffer block;
		IntBuffer intBuffer;
		float[] nextRWR;

		public RandomWalkWithRestartThread(MappedByteBuffer block, float[] nextRWR) {
			this.block = block;
			this.nextRWR = nextRWR;
			this.intBuffer = block.asIntBuffer().asReadOnlyBuffer();
		}

		@Override
		public Boolean call() throws Exception {
			block.position(0);
			while (intBuffer.hasRemaining()) {
				int source = intBuffer.get();
				int target = intBuffer.get();
				nextRWR[target] += (1 - RESISTANCE) * rwr[source] * oneOverOutDegree[source];
			}

			return true;
		}
	}
	
	// author kaeser
	/**
	 * @param k
	 * @return the top k close nodes after random walk;
	 */
	public String topCloseNode(int k) {
		PriorityQueue<Double> topK = new PriorityQueue<Double>();
		for (float rwrValue : rwr) {
			if (topK.size() < k) {
				topK.add((double) rwrValue);
			} else {
				if (rwrValue > topK.peek()) {
					topK.poll();
					topK.add((double) rwrValue);
				}
			}
		}

		String ret = "";
		double sum = 0;
		while (!topK.isEmpty()) {
			double v = topK.poll();
			ret = (v + "\n") + ret;
			sum += v;
		}
		
		System.out.println("Proximity sum: "+sum);
		return ret;
	}
	
	// author: kaeser
	public Set<Integer> getOneStepNeighbor(int sourceNode) {
		Set<Integer> neighborSet = new ConcurrentSkipListSet<>();
		
		final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);
		CompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(threadPool);
	
		for (MappedByteBuffer block : byteBuffer) {
			final Callable<Boolean> thread = new OneStepNeighborThread(block, neighborSet, sourceNode);
			ecs.submit(thread);
		}
		
		try {
			for (int i = 0; i < blockNum; i++) {
				ecs.take();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		threadPool.shutdown();
		return neighborSet;
	}
	
	// author: kaeser
	private class OneStepNeighborThread implements Callable<Boolean> {
		MappedByteBuffer block;
		IntBuffer intBuffer;
		int sourceNode;
		Set<Integer> neighborSet;

		public OneStepNeighborThread(MappedByteBuffer block, Set<Integer> neighborSet, int sourceNode) {
			this.block = block;
			this.neighborSet = neighborSet;
			this.sourceNode = sourceNode;
			this.intBuffer = block.asIntBuffer().asReadOnlyBuffer();
		}

		@Override
		public Boolean call() throws Exception {
			block.position(0);
			while (intBuffer.hasRemaining()) {
				int src = intBuffer.get();
				int dst = intBuffer.get();

				if (src == sourceNode) {
					neighborSet.add(dst);
				}
			}
			return true;
		}
	}
	
	// author: kaeser
	public List<Integer> getOneStepNeighborFromIndex(int sourceNode, String edgeFile, String indexFile) {
		List<Integer> neighborList = new ArrayList<>();

		try {
			RandomAccessFile idxFile = new RandomAccessFile(indexFile, "r");
			MappedByteBuffer bb = idxFile.getChannel().map(MapMode.READ_ONLY, sourceNode * 12, 12);
			long offset = bb.getLong();
			int degree = bb.getInt();
			bb.clear();
			idxFile.close();
			
			RandomAccessFile edgFile = new RandomAccessFile(edgeFile, "r");
			bb = edgFile.getChannel().map(MapMode.READ_ONLY, offset * 4, degree * 8);
			IntBuffer ib = bb.asIntBuffer().asReadOnlyBuffer();
			
			while (ib.hasRemaining()) {
				ib.get();
				neighborList.add(ib.get());
			}
			
			edgFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return neighborList;
	}
	
	// author: kaeser
	public Set<Integer> getTwoStepNeighbor(int sourceNode) {
		Set<Integer> oneStepNeighborSet = new ConcurrentSkipListSet<>();
		Set<Integer> twoStepNeighborSet = new ConcurrentSkipListSet<>();
		
		final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);
		CompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(threadPool);
	
		// one step neighbor
		for (MappedByteBuffer block : byteBuffer) {
			final Callable<Boolean> thread = new OneStepNeighborThread(block, oneStepNeighborSet, sourceNode);
			ecs.submit(thread);
		}
		
		try {
			for (int i = 0; i < blockNum; i++) {
				ecs.take();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println(oneStepNeighborSet.size());
//		System.out.println(oneStepNeighborSet);
		
		// two step neighbor
		oneStepNeighborSet.add(sourceNode);
		for (MappedByteBuffer block : byteBuffer) {
			final Callable<Boolean> thread = new TwoStepNeighborThread(block, twoStepNeighborSet, oneStepNeighborSet);
			ecs.submit(thread);
		}
		
		try {
			for (int i = 0; i < blockNum; i++) {
				ecs.take();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		threadPool.shutdown();
		return twoStepNeighborSet;
	}
	
	// author: kaeser
	private class TwoStepNeighborThread implements Callable<Boolean> {
		MappedByteBuffer block;
		IntBuffer intBuffer;
		Set<Integer> oneStepNeighborSet;
		Set<Integer> twoStepNeighborSet;

		public TwoStepNeighborThread(MappedByteBuffer block,
				Set<Integer> twoStepNeighborSet, Set<Integer> oneStepNeighborSet) {
			this.block = block;
			this.oneStepNeighborSet = oneStepNeighborSet;
			this.twoStepNeighborSet = twoStepNeighborSet;
			this.intBuffer = block.asIntBuffer().asReadOnlyBuffer();
		}

		@Override
		public Boolean call() throws Exception {
			block.position(0);
			while (intBuffer.hasRemaining()) {
				int src = intBuffer.get();
				int dst = intBuffer.get();

				//if (src == 2244270) System.out.println("Dst: "+dst);
				
				if (oneStepNeighborSet.contains(src)) {
					twoStepNeighborSet.add(dst);	
				}
				else {
					while (true) {
						if (intBuffer.hasRemaining() == false)
							return true;
						int s = intBuffer.get();
						int t = intBuffer.get();
						
						if (s != src) {
							int k = intBuffer.position() - 2;
							intBuffer.position(k);
							break;
						}
					}
				}
			}
			return true;
		}
	}
	
	
	// author: kaeser
	public Set<Integer> getTwoStepNeighborFromIndex(int sourceNode, String edgeFile, String indexFile) {
		List<Integer> oneStepNeighborList = new ArrayList<>();

		oneStepNeighborList = getOneStepNeighborFromIndex(sourceNode, edgeFile, indexFile);
//		System.out.println(oneStepNeighborList.size());

		// two step neighbor
		Set<Integer> twoStepNeighborSet = new ConcurrentSkipListSet<>(oneStepNeighborList);
		
		final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);
		CompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(threadPool);

		for (int i : oneStepNeighborList) {
			final Callable<Boolean> thread = new TwoStepNeighborFromIndexThread(i,
					twoStepNeighborSet, edgeFile, indexFile);
			ecs.submit(thread);
		}

		try {
			for (int i = 0; i < oneStepNeighborList.size(); i++) {
				ecs.take();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		threadPool.shutdown();
		return twoStepNeighborSet;
	}

	// author: kaeser
	private class TwoStepNeighborFromIndexThread implements Callable<Boolean> {
		MappedByteBuffer block;
		IntBuffer intBuffer;
		Set<Integer> twoStepNeighborSet;

		public TwoStepNeighborFromIndexThread(int sourceNode,
				Set<Integer> twoStepNeighborSet, String edgeFile,
				String indexFile) {
			try {
				RandomAccessFile idxFile = new RandomAccessFile(indexFile, "r");
				block = idxFile.getChannel().map(MapMode.READ_ONLY, sourceNode * 12, 12);
				long offset = block.getLong();
				int degree = block.getInt();
//				System.out.println("1Nh: "+sourceNode+" offset: "+offset+" Deg: "+degree);
				block.clear();
				idxFile.close();

				RandomAccessFile edgFile = new RandomAccessFile(edgeFile, "r");
				block = edgFile.getChannel().map(MapMode.READ_ONLY, offset * 4, degree * 8);
				intBuffer = block.asIntBuffer().asReadOnlyBuffer();
				edgFile.close();
				
				this.twoStepNeighborSet = twoStepNeighborSet;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public Boolean call() throws Exception {
			//block.position(0);
			while (intBuffer.hasRemaining()) {
				intBuffer.get();
				twoStepNeighborSet.add(intBuffer.get());
			}
			return true;
		}
	}
	
	// does not work as expected!!
	// unsafe array test
	/*
	// return number of connected component
	public void getAllConnectedComponentUnsafe() {
		System.out.println(nodeNum);
		unsafeCcId = new UnsafeBigArray(nodeNum * UNIT_SIZE); // size in bytes
		for (int i = 0; i < nodeNum; i++) {
			unsafeCcId.setInt(i, i);
		}
		AtomicBoolean converged = new AtomicBoolean(false);
		int iteration = 0;
		System.out.println("blockNum: " + blockNum);
		final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);
		CompletionService<Boolean> ecs = new ExecutorCompletionService<Boolean>(threadPool);
		// keep scanning till converged
		Timer timer = new Timer();
		while (!converged.get()) {
			System.out.println("Iteration " + iteration);
			converged.set(true);
			// ---------------MultiThreading-----------
			for (MappedByteBuffer block : byteBuffer) {
				final Callable<Boolean> thread = new BlockIteratingThreadUnsafe(block, unsafeCcId, converged);
				ecs.submit(thread);
			}
			// wait till all threads finish
			try {
				for (int i = 0; i < blockNum; i++) {
					ecs.take();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// for (int i : ccId) {
			// System.out.print(i + " ");
			// }
			// System.out.println();
			iteration++;
		}
		threadPool.shutdown();
		timer.showTimeElapsed("Found all CCs");
		System.out.printf("Iteration #: %d%n", iteration);
	}

	private class BlockIteratingThreadUnsafe implements Callable<Boolean> {
		MappedByteBuffer block;
		AtomicBoolean converged;
		UnsafeBigArray unsafeCcId;

		public BlockIteratingThreadUnsafe(MappedByteBuffer block, UnsafeBigArray unsafeCcId, AtomicBoolean converged) {
			this.block = block;
			this.converged = converged;
			this.unsafeCcId = unsafeCcId;
		}

		@Override
		public Boolean call() throws Exception {
			boolean blockConverged = false;
			while (!blockConverged) {
				blockConverged = true;
				block.position(0);
				while (block.position() < block.limit()) {
					int source = block.getInt();
					int target = block.getInt();
					int ccIdSource = unsafeCcId.getInt(source);
					int ccIdTarget =  unsafeCcId.getInt(target);
					//System.out.println("edge: "+source+" "+target+" "+ccIdSource+" "+ccIdTarget);
					if (ccIdSource != ccIdTarget) {
						blockConverged = false;
						if (ccIdSource > ccIdTarget) {
							unsafeCcId.setInt(source, ccIdTarget);
						}
						else {
							unsafeCcId.setInt(target, ccIdSource);
						}
					}
				}
				if (blockConverged == false) {
					converged.set(false);
				}
			}
			return converged.get();
		}
	}

	public int countCCNumUnsafe() {
		 Set<Integer> ccSet = new HashSet<>();
		 int ccNum = 0;
		 for (int i = 0; i < nodeNum; i++) {
			 int v = unsafeCcId.getInt(i);
			 if (!ccSet.contains(v)) {
				 ccNum++;
				 ccSet.add(v);
			 }
			 // System.out.println(i+" "+ccId[i]);
		 }
		 System.out.println("total Node: " + nodeNum);
		 return ccNum;
		
//		// TEMP ALGORITHM FOR TWITTER GRAPH		
//		for (int i = 0; i < ccId.length; i++) {
//			if (ccId[i] != 0) {
//				return -1;
//			}
//		}
//		return 0;
//		// DONE WITH TEMP ALG
	}
	*/
}
