package edu.gatech.memory_mapping_alg;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class EdgeListIndex {

//	assumptions: nodes are contiguous, no breakage in node ids
//	assumptions: edge list is sorted by node ids
//	
	public static void getEdgeListIndex(String binFileName, String idxFileName) {
		try {
			RandomAccessFile srcFile = new RandomAccessFile(binFileName, "r");
			RandomAccessFile idxFile = new RandomAccessFile(idxFileName, "rw");

			
//			DataOutputStream idxFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(idxFileName)));
			
			long binFileSize = srcFile.length();
			long bufferLimit = 2097152000; // 2000 MB 
		
			int currentNode = 0;
			int currentNodeDegree = 0;
			long currentNodeOffset = 0;
			long currentPosition = 0;
			
			long loadFrom = 0;
			
			while (loadFrom < binFileSize) {
				long remaining = Math.min(binFileSize - loadFrom, bufferLimit);
				ByteBuffer srcbb = srcFile.getChannel().map(MapMode.READ_ONLY, loadFrom, remaining);
				loadFrom += remaining;
				
				while (srcbb.hasRemaining()) {
					int src = srcbb.getInt();
					srcbb.getInt();
					
					if (src != currentNode) {
					
//						if (src != currentNode+1) System.out.println(currentNode+1);
						idxFile.seek(currentNode*12);
						idxFile.writeLong(currentNodeOffset);
						idxFile.writeInt(currentNodeDegree);
						currentNode = src;
						currentNodeDegree = 0;
						currentNodeOffset = currentPosition;
						
//						while(currentNode < src) {
//							//System.out.println(src+"----"+currentNode);
//							idxFile.writeLong(-1);
//							idxFile.writeInt(-1);						
//							++currentNode;
//						}
					}
					
					++currentNodeDegree;
					currentPosition += 2;
				}
			}
			
			srcFile.close();
			idxFile.close();
			
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		getEdgeListIndex("C://Users//Lupu//Downloads//LiveJournal//LiveJournal.txt.bin", "C://Users//Lupu//Downloads//LiveJournal//LiveJournal.index.bin");
	}
}
