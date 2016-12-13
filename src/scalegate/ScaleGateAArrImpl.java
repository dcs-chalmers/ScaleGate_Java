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

import java.util.Random;

public class ScaleGateAArrImpl implements ScaleGate {

	final int maxlevels;
	SGNodeAArrImpl head;
	final SGNodeAArrImpl tail;

	final int numberOfWriters;
	final int numberOfReaders;
	// Arrays of source/reader id local data
	WriterThreadLocalData[] writertld;
	ReaderThreadLocalData[] readertld;


	public ScaleGateAArrImpl (int maxlevels, int writers, int readers) {
		this.maxlevels = maxlevels;

		this.head = new SGNodeAArrImpl(maxlevels, null, null, -1);
		this.tail = new SGNodeAArrImpl(maxlevels, null, null, -1);

		for (int i = 0; i < maxlevels; i++)
			head.setNext(i, tail);

		this.numberOfWriters = writers;
		this.numberOfReaders = readers;

		writertld = new WriterThreadLocalData[numberOfWriters];
		for (int i=0; i < numberOfWriters; i++) {
			writertld[i] = new WriterThreadLocalData(head);
		}

		readertld = new ReaderThreadLocalData[numberOfReaders];
		for (int i=0; i< numberOfReaders; i++) {
			readertld[i] = new ReaderThreadLocalData(head);
		}
		
		//This should not be used again, only the writer/reader-local variables
		head = null;
	}

	@Override
	/*
	 * (non-Javadoc)
	 */
	public SGTuple getNextReadyTuple(int readerID) {
		SGNodeAArrImpl next = getReaderLocal(readerID).localHead.getNext(0);

		if (next != tail && !next.isLastAdded()) {
			getReaderLocal(readerID).localHead = next;
			return next.getTuple();
		}
		return null;
	}

	@Override
	// Add a tuple 
	public void addTuple(SGTuple tuple, int writerID) {
		this.internalAddTuple(tuple, writerID);
	}

	private void insertNode(SGNodeAArrImpl fromNode, SGNodeAArrImpl newNode, final SGTuple obj, final int level) {
		while (true) {
			SGNodeAArrImpl next = fromNode.getNext(level);
			if (next == tail || next.getTuple().compareTo(obj) > 0) {
				newNode.setNext(level, next);
				if (fromNode.trySetNext(level, next, newNode)) {
					break;
				}
			} else {
				fromNode = next;
			}
		}
	}

	private SGNodeAArrImpl internalAddTuple(SGTuple obj, int inputID) {
		int levels = 1;
		WriterThreadLocalData ln = getWriterLocal(inputID);

		while (ln.rand.nextBoolean() && levels < maxlevels)
			levels++;


		SGNodeAArrImpl newNode = new SGNodeAArrImpl (levels, obj, ln, inputID);
		SGNodeAArrImpl [] update = ln.update;
		SGNodeAArrImpl curNode = update[maxlevels - 1];

		for (int i = maxlevels - 1; i >= 0; i--) {
			SGNodeAArrImpl tx = curNode.getNext(i);

			while (tx != tail && tx.getTuple().compareTo(obj) < 0) {
				curNode = tx;
				tx = curNode.getNext(i);
			}

			update[i] = curNode;
		}

		for (int i = 0; i < levels; i++) {
			this.insertNode(update[i], newNode, obj, i);
		}

		ln.written = newNode;
		return newNode;
	}

	private WriterThreadLocalData getWriterLocal(int writerID) {
		return writertld[writerID];
	}

	private ReaderThreadLocalData getReaderLocal(int readerID) {
		return readertld[readerID];
	}
	protected class WriterThreadLocalData {
		// reference to the last written node by the respective writer
		volatile SGNodeAArrImpl written; 
		SGNodeAArrImpl [] update;
		final Random rand;

		public WriterThreadLocalData(SGNodeAArrImpl localHead) {
			update = new SGNodeAArrImpl[maxlevels];
			written = localHead;
			for (int i=0; i < maxlevels; i++) {
				update[i]= localHead;
			}	    
			rand = new Random();
		}
	}

	protected class ReaderThreadLocalData {
		SGNodeAArrImpl localHead;

		public ReaderThreadLocalData(SGNodeAArrImpl lhead) {
			localHead = lhead;
		}
	}
}
