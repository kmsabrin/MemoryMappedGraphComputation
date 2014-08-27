package edu.gatech.memory_mapping_alg;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel.MapMode;

public class HybridBigArray {

	int numberOfTotalItems;
	int numberOfInmemoryItems;
	int numberOfIndiskItems;
	int itemUnitSize = 4;

	int[] inmemoryStore;
	RandomAccessFile randomAccessFile;
	String fileName;
	ByteBuffer bb;
	IntBuffer intBuffer;

	HybridBigArray(int numberOfTotalItems, int numberOfInmemoryItems, String fileName) {
		this.numberOfTotalItems = numberOfTotalItems;
		this.numberOfInmemoryItems = numberOfInmemoryItems;
		this.numberOfIndiskItems = numberOfTotalItems - numberOfInmemoryItems;
		this.fileName = fileName;

		try {
			inmemoryStore = new int[numberOfInmemoryItems];

			randomAccessFile = new RandomAccessFile(fileName, "rw");
			bb = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0,
					numberOfIndiskItems * itemUnitSize);
			intBuffer = bb.asIntBuffer();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (OutOfMemoryError e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	void cleanup() {
		try {
			bb.clear();
			intBuffer.clear();

			// randomAccessFile.setLength(0);
			randomAccessFile.getChannel().close();
			randomAccessFile.close();

			// issues: not being able to delete the file, since it is still
			// mapped!
			System.gc();
			File f = new File(fileName);
			f.deleteOnExit();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	int getItem(int index) throws IndexOutOfBoundsException {
//		if (index >= numberOfTotalItems) {
//			throw new IndexOutOfBoundsException();
//		}

		if (index < numberOfInmemoryItems) {
			return inmemoryStore[index];
		} else {
			return intBuffer.get(index - numberOfInmemoryItems);
		}
	}

	void setItem(int index, int value) {
//		if (index >= numberOfTotalItems) {
//			throw new IndexOutOfBoundsException();
//		}

		if (index < numberOfInmemoryItems) {
			inmemoryStore[index] = value;
		} else {
			intBuffer.put(index - numberOfInmemoryItems, value);
		}
	}

	public static void main(String[] args) {

		int nItems = 100;
		int nMemoryItems = 50;

		HybridBigArray bigArray = new HybridBigArray(nItems, nMemoryItems,
				"E:\\diskArray.bin");

		// bigArray.setItem(100, 1);

		for (int i = 0; i < nItems; ++i) {
			bigArray.setItem(i, i * 2 + 1);
		}

		for (int i = 0; i < nItems; ++i) {
			System.out.println(bigArray.getItem(i));
		}

		bigArray.cleanup();
	}
}
