/**
 * 
 */
package com.skc.hadoop.bilionprime;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author sitakant
 *
 */
public class DataGenerator {
	
	public static void main(String[] args) {
		
		ExecutorService executerService = Executors.newScheduledThreadPool(100);
		
		Runnable _worker = new PrepareData(1);
		executerService.execute(_worker);
		for (int i = 1; i <= 10; i++) {
			Runnable worker = new PrepareData(i*100);
			executerService.execute(worker);
		}
		
		executerService.shutdown();
		while(!executerService.isTerminated()) {
			//System.out.println("Awaiting to close all the thread");
		}
		
		System.out.println("All are done");
	}
}


class PrepareData implements Runnable{
	
	private int offset; 
	
	public PrepareData(int offset) {
		this.offset = offset;
	}

	public void run() {
		writeToFile();
	}

	protected void writeToFile() {

		String myData = "";
		for (int i = 0; i < offset* 100; i++) {
			myData += (offset+i)+",";
		}
		System.out.println(Thread.currentThread().getName()+"\t"+myData);
		
		File file = new File("C:\\Users\\chaudhsi\\eclipse-workspace\\HadoopLabs\\BilionPrimeNumber\\target\\"+Thread.currentThread().getName()+"-"+UUID.randomUUID().toString()+".txt");
		  
		//Create the file
		try {
			file.createNewFile();
			FileWriter writer = new FileWriter(file);
			writer.write(myData);
			writer.close(); 
			Thread.sleep(2000);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

}