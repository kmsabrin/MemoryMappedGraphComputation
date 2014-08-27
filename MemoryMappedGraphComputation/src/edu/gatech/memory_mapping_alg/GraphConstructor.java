package edu.gatech.memory_mapping_alg;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;

/**
 * this is a class that takes in edge set file and output a graph by default
 * using JGraphT/NEO4J/MEMORY_MAPPING Try to interpret node as integers,
 * otherwise treat everything as strings it is not supported to store a
 * decomposed string graph
 * 
 * @author Zhiyuan "Jerry" Lin
 * 
 */
public class GraphConstructor {
	public static int currentIndex = 0;
	public final static long BUFFER_LIMIT = 1048576000L;//1000 MB in byte //2147483647+1;
	public final static String SEPARATOR = " ";//"	";

	/**
	 *
	 * to build up vertices and edges in an empty graph
	 *
	 * @param filename
	 * @param isDirected
	 * @param startWithMaxIdAndEdgeNum true if the first line consists of two numbers which are maxID and edge num
	 */
	@SuppressWarnings("unused")
	public static void makeIntegerGraph(String filename, boolean isDirected, boolean startWithMaxIdAndEdgeNum) {
		BufferedReader inputBufferedReader = null;
		File binFile = null;
		long maxIdFromFile = 0L;
		long edgeNumFromFile = 0L;
		
		try {
			inputBufferedReader = new BufferedReader(new FileReader(filename));
			String sCurrentLine;
			binFile = new File(filename + ".bin");
			Map<Integer, Integer> indexMap = new HashMap<>((int) maxIdFromFile);
			long edgeNum = 0;
			if (!binFile.exists()) {
				RandomAccessFile raf = new RandomAccessFile(binFile, "rw");
				int v1;
				int v2;
				String[] s;
				
				long bufferCount = 0L;
				int currentLoad = 0;
				//the max number of int each time it will write into the disk
				long loadLimit = BUFFER_LIMIT/4;
				int[] dataToWriteIntoDisk = new int[(int) (loadLimit)];
				//deal with graph
				if (startWithMaxIdAndEdgeNum){
					sCurrentLine = inputBufferedReader.readLine();
					s = sCurrentLine.split(SEPARATOR, 2);
					maxIdFromFile = Integer.parseInt(s[0]);
					edgeNumFromFile = Integer.parseInt(s[1]);
					System.out.print("Special First Line: "+maxIdFromFile+" "+edgeNumFromFile);
					
				}
				
				// for debugging
				Timer timer = new Timer();
				while ((sCurrentLine = inputBufferedReader.readLine()) != null) {
//					if (sCurrentLine.charAt(0) == '#' || sCurrentLine.charAt(0) == '%') {
//						continue;
//					}
					
					s = sCurrentLine.split(SEPARATOR, 2);
					v1 = Integer.parseInt(s[0]);
//					if (!indexMap.containsKey(v1)) {
//						indexMap.put(v1, currentIndex);
//						currentIndex++;
//					}
//					v1 = indexMap.get(v1);

					v2 = Integer.parseInt(s[1]);
//					if (!indexMap.containsKey(v2)) {
//						indexMap.put(v2, currentIndex);
//						currentIndex++;
//					}
//					v2 = indexMap.get(v2);

//					// guarantee that the source id is smaller than target id
//					if (!isDirected && v1 > v2) {
//						Integer temp = v1;
//						v1 = v2;
//						v2 = temp;
//					}

					dataToWriteIntoDisk[currentLoad] = v1;
					currentLoad++;
					dataToWriteIntoDisk[currentLoad] = v2;
					currentLoad++;

//					System.out.println("currentline: "+sCurrentLine);
//					System.out.println("actually get "+v1+" "+v2);
//					System.out.println(bb.position());
					if (currentLoad >= loadLimit) {
						timer.showTimeElapsed("reading from txt");
						ByteBuffer bb = raf.getChannel().map(MapMode.READ_WRITE, bufferCount*BUFFER_LIMIT, BUFFER_LIMIT);
						IntBuffer intBuffer = bb.asIntBuffer();
				        
						intBuffer.position(0);
						intBuffer.put(dataToWriteIntoDisk);
						//						for (int data: dataToWriteIntoDisk){
//							bb.putInt(data);
//						}
						intBuffer.clear();
						bb.clear();
						bufferCount++;
						edgeNum += currentLoad/2;
						currentLoad = 0;
						timer.showTimeElapsed("write into bin");
						System.out.println(bufferCount*BUFFER_LIMIT*9.53674316e-7+"MB has been written in bin file.");
						System.out.println("overall position:" + raf.getChannel().position());
					}

				}
				
				if (currentLoad != 0) {
					timer.showTimeElapsed("reading from txt");
					ByteBuffer bb = raf.getChannel().map(MapMode.READ_WRITE, bufferCount*BUFFER_LIMIT, currentLoad*4);
					bb.position(0);
					for (int i = 0; i< currentLoad; i++){
						bb.putInt(dataToWriteIntoDisk[i]);
					}
					bb.clear();
					bufferCount++;
					edgeNum += currentLoad/2;
					currentLoad = 0;
					timer.showTimeElapsed("write into bin");
					System.out.println(bufferCount*BUFFER_LIMIT*9.53674316e-7+"MB has been written in bin file.");
					System.out.println("overall position:" + raf.getChannel().position());
				}
				
//				System.out.println(raf.length());
//				bb.clear();
//				bb = null;
//				System.gc();
				raf.getChannel().close();
				raf.close();
				System.gc();

				// for finding max id
				int maxId = 0;
				for (int i : indexMap.keySet()) {
					if (i > maxId) {
						maxId = i;
					}
				}
				System.out.println("Max id: " + maxId);
				System.out.println("Max id from file: " + maxIdFromFile);

//				raf = new RandomAccessFile(binFile, "rw");
//				raf.setLength(edgeNum * 2 * MemoryMappingGraph.UNIT_SIZE);
//				System.out.println(raf.length());
//				raf.close();
				
				System.out.println("Done Converting To Binary File");
			}
		} catch (Exception e) {
			if (binFile != null) {
				binFile.delete();
			}
			e.printStackTrace();
		} finally {
			try {
				if (inputBufferedReader != null) {
					inputBufferedReader.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
