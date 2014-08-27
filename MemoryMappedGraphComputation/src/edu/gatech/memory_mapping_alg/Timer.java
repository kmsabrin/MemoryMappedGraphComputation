package edu.gatech.memory_mapping_alg;
/**
 * 
 * @author Zhiyuan "Jerry" Lin
 * 
 */
public class Timer {
	private long lastTime;

	public Timer() {
		reset();
	}

	public void showTimeElapsed(String comment) {
		System.out.println(comment + ": " + (System.currentTimeMillis() - lastTime) + " milliseconds");
		lastTime = System.currentTimeMillis();
	}

	public long getTimeElapsed(){
		long timeElapsed = System.currentTimeMillis() - lastTime;
		lastTime = System.currentTimeMillis();
		return timeElapsed;
	};

	public void reset() {
		lastTime = System.currentTimeMillis();
	}
}