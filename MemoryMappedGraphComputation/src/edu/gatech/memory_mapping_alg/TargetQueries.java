package edu.gatech.memory_mapping_alg;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class TargetQueries {

	public static Set<Integer> getTwoStepNeighbor(File binFile, int sourceNode) {
		
		Set<Integer> oneStepNeighborSet;
		Set<Integer> twoStepNeighborSet;
		
		oneStepNeighborSet = getOneStepNeighbor(binFile, sourceNode, null, false);
		
		System.out.println(oneStepNeighborSet);

		oneStepNeighborSet.add(sourceNode);
		twoStepNeighborSet = getOneStepNeighbor(binFile, -1, oneStepNeighborSet, true);
		
		System.out.println("Number of TwoStopNeighbor of "+sourceNode+" is "+twoStepNeighborSet.size());
		
		return twoStepNeighborSet;
	}
	
	
	public static Set<Integer> getOneStepNeighbor(File binFile, int sourceNode, Set<Integer> sourceSet, boolean useSourceSet) {
		Set<Integer> neighborSet = new HashSet<>();
		RandomAccessFile randomAccessFile;
		ByteBuffer bb;
		IntBuffer intBuffer;
		
		try {
			randomAccessFile = new RandomAccessFile(binFile, "r");
//			System.out.println(randomAccessFile.length());
			
			long fileSizeInBytes = randomAccessFile.length();
			long maxFileSizeInBytesForMapping = 2000000000L;
			
			int nBlockRead = 0;
			while (fileSizeInBytes >= maxFileSizeInBytesForMapping) {
				
				bb = randomAccessFile.getChannel().map(MapMode.READ_ONLY, nBlockRead * maxFileSizeInBytesForMapping, maxFileSizeInBytesForMapping);
				intBuffer = bb.asIntBuffer();
				
				while (intBuffer.hasRemaining()) {
					int src = intBuffer.get();
					int dst = intBuffer.get();
					
					if (useSourceSet == false) {
						if (src == sourceNode) {
							neighborSet.add(dst);
						}
					}
					else {
						if (sourceSet.contains(src)) {
							neighborSet.add(dst);
						}
					}
				}
				
				fileSizeInBytes -= maxFileSizeInBytesForMapping; 
				++nBlockRead;
			}
			
			if (fileSizeInBytes > 0) {
				bb = randomAccessFile.getChannel().map(MapMode.READ_ONLY, nBlockRead * maxFileSizeInBytesForMapping, fileSizeInBytes);
				intBuffer = bb.asIntBuffer();
				
				while (intBuffer.hasRemaining()) {
					int src = intBuffer.get();
					int dst = intBuffer.get();
					
					if (useSourceSet == false) {
						if (src == sourceNode) {
							neighborSet.add(dst);
						}
					}
					else {
						if (sourceSet.contains(src)) {
							neighborSet.add(dst);
						}
					}
				}
			}
			
			System.out.println("Number of OneStopNeighbor of "+sourceNode+" is "+neighborSet.size());
			
			randomAccessFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			
		}

		return neighborSet;
	}

	public static void main(String[] args) {

		Timer timer = new Timer();
		
//		getOneStepNeighbor(new File("C://Users//Lupu//Downloads//LiveJournal//LiveJournal.txt.bin"), 1000, null, false);
//		
//		timer.showTimeElapsed("OneStopNeighbor");
		

		timer.reset();
		
		getTwoStepNeighbor(new File("C://Users//Lupu//Downloads//LiveJournal//LiveJournal.txt.bin"), 10000);
		
		timer.showTimeElapsed("TwoStopNeighbor");
	}
}
