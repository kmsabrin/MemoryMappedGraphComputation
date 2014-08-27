package edu.gatech.memory_mapping_alg;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

class UnsafeBigArray {
    private final static int BYTE_SIZE = 1;
    private final static int INTEGER_SIZE = 4;
    private long size;
    private long address;

    public UnsafeBigArray(long size) {
        this.size = size;
        this.address = getUnsafe().allocateMemory(size * BYTE_SIZE);
    }

    public void setByte(long i, byte value) {
        getUnsafe().putByte(address + i * BYTE_SIZE, value);
    }

    public int getByte(long idx) {
        return getUnsafe().getByte(address + idx * BYTE_SIZE);
    }
    
    public void setInt(long i, int value) {
        getUnsafe().putInt(address + i * INTEGER_SIZE, value);
    }

    public int getInt(long idx) {
        return getUnsafe().getInt(address + idx * INTEGER_SIZE);
    }

    public long size() {
        return size;
    }
    
    public static Unsafe getUnsafe() {
    	Field f = null;
    	try {
        	f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe)f.get(null);
        } catch(Exception e) { return null; }
    }
    
    public static void main(String[] args) {
    	
//    	long SUPER_SIZE = (long)Integer.MAX_VALUE * 2;
//    	UnsafeBigArray array = new UnsafeBigArray(SUPER_SIZE);
//    	System.out.println("Array size:" + array.size()); // 4294967294
//    	int sum = 0;
//    	for (int i = 0; i < 100; i++) {
//    	    array.setInt((long)Integer.MAX_VALUE + i, 3);
//    	    sum += array.getInt((long)Integer.MAX_VALUE + i);
//    	}
//    	System.out.println("Sum of 100 elements:" + sum);  // 300
    	
    	int nItem = 10000000;
    	
    	Timer timer = new Timer();
    	UnsafeBigArray array = new UnsafeBigArray(nItem * 4L);
    	for (int i = 0; i < nItem; ++i) {
    		array.setInt(i, 1);
    	}
    	int sum = 0;
    	for (int i = 0; i < nItem; ++i) {
    		int j = array.getInt(i);
    		sum += j;
    	}
    	System.out.println(sum);
    	timer.showTimeElapsed("Unsafe time");
    	
    
    	int arr[] = new int[nItem];
    	for (int i = 0; i < nItem; ++i) {
    		arr[i] = 1;
    	}
    	sum = 0;
    	for (int i = 0; i < nItem; ++i) {
    		int j = arr[i];
    		sum += j;
    	}
    	System.out.println(sum);
    	timer.showTimeElapsed("Safe time");
    	
    }
}
