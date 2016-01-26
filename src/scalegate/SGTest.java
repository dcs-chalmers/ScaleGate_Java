/*  Copyright (C) 2015  Ioannis Nikolakopoulos,  
 * 			Daniel Cederman, 
 * 			Vincenzo Gulisano,
 * 			Marina Papatriantafilou,
 * 			Philippas Tsigas
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Contact: Ioannis (aka Yiannis) Nikolakopoulos ioaniko@chalmers.se
 *  	     Vincenzo Gulisano vincenzo.gulisano@chalmers.se
 *
 */

package scalegate;

import java.util.concurrent.atomic.AtomicInteger;

public class SGTest {
    
    static final int TEST_SIZE = 1000001;
    static int PRODUCERS, CONSUMERS;
    
    static ScaleGate tgate;
    static AtomicInteger barrier;
    
    public static void main(String[] args) throws InterruptedException {

	PRODUCERS = 2;
	CONSUMERS = 2;
	SGTest theTest = new SGTest();
	
	tgate = new ScaleGateAArrImpl(4, PRODUCERS, CONSUMERS);
	barrier = new AtomicInteger(PRODUCERS + CONSUMERS);

	System.out.println(" Hello world! Starting threads...");
	
	Thread[] producers = new Thread[PRODUCERS];
	Thread[] consumers = new Thread[CONSUMERS];
	
	for (int i=0; i < PRODUCERS; i++) {
	    producers[i] = new Thread(theTest.new Produce(i));
	}	
	for (int i=0; i< CONSUMERS; i++) {
	    consumers[i] = new Thread(theTest.new Consume(i));
	}
	
	for (int i=0; i< PRODUCERS; i++) {
	    producers[i].start();
	}	
	for (int i=0; i < CONSUMERS; i++) {
	    consumers[i].start();
	}
	
	
	for (Thread t : producers) {
	    t.join();
	}
	
	for (Thread t : consumers) {
	    t.join();
	}
	
	System.out.println("Done!");
    }
    
    class Produce implements Runnable {

	int id;
	
	Produce (int id) {
	    this.id = id;
	}
	
	@Override
	public void run() {
	    //Initialization i.e. ensure one tuple from each producer
	    TestTupleImpl foo = new TestTupleImpl(1 + id);
	    tgate.addTuple(foo, id);
	    
	    //Synch with the rest
	    barrier.getAndDecrement();
	    while (barrier.get() != 0);
	    
	    for (int i= 1 + PRODUCERS + id; i < TEST_SIZE; i++) {
		foo = new TestTupleImpl(i);
		tgate.addTuple(foo, id);
	    }
	    
	    System.out.println("Producer thread " + id + " done");
	}
	
    }
    
    class Consume implements Runnable {

	int id;
	
	Consume (int id) {
	    this.id = id;
	}
	
	@Override
	public void run() {
	    
	    //Make sure the system is correctly initialized, i.e. one tuple from each producer exists
	    barrier.getAndDecrement();
	    while (barrier.get() != 0);
	    
	    // Get the first tuple so that we can later check assertions for sanity checks
	    SGTuple cur, prev;
	    do {
		prev = tgate.getNextReadyTuple(id);
	    } while (prev == null);
	    
	    long sum = prev.getTS();
	    
	    while (true) {
		cur = tgate.getNextReadyTuple(id);
		if (cur != null) {
		    assert(prev.getTS() + 1 == cur.getTS());
		    sum += cur.getTS();
		    
		    if (cur.getTS() >= TEST_SIZE - 1 - PRODUCERS) //last tuple, break
			break;
		    prev = cur;
		}
	    }
	    
	    System.out.println("Consumer thread " + id + " done " + sum);
	}
	
    }

}
