/**
 * This file implements the BufMgr class and all its functions. In essence, the BufMgr uses an LRU
 * clock replacement algorithm that takes in a page request and if it is in buffer pool BufMgr
 * returns pointer that to page. If the page is not in the pool, it frees a frame and gets the page
 * to the frame from the disk.
 *
 * CS 564 Group 55
 * James Ma: 9079648441
 * Keyi Wang: 9080306518
 * Ayman Alzomaili: 9075944141
 * Brandon Erickson: 9079060738
 *
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include <stdio.h>
#include <inttypes.h>

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

	// Initializes variables stored in the buffer table
	for (FrameId i = 0; i < bufs; i++) {
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	}

	bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
	hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	clockHand = bufs - 1;
}

/**
 * Flushes out dirty pages, deallocates buffer pool and BufDesc table
 */
BufMgr::~BufMgr() {
	// Flushes all files
	for (FrameId i = 0; i < numBufs; i++) {
		if(bufDescTable[i].dirty) {
			flushFile(bufDescTable[i].file);
		}
	}
	
	// Deletes variables used in the file
	delete[] bufPool;
	delete[] bufDescTable;
	delete hashTable;
}

/**
 * Advances the clock pointer by one
 */
void BufMgr::advanceClock() {
	clockHand = (clockHand+1)%numBufs;
}

/**
 * Allocates a free frame using clock algorithm
 * Writes dirty page back to disk if applicable
 * @param frame pointer to frame
 * @throws BufferExceededException if all buffer frames are pinned
 */
void BufMgr::allocBuf(FrameId & frame){
	int count=0;
	while(true){
		advanceClock();
		// If a frame with pinned pages
		if(bufDescTable[clockHand].pinCnt>0){
			count++;
			// If all frames are pinned
			if((unsigned)count==numBufs){
				throw BufferExceededException();
			}
		}else{
			count=0;
		}
	
		/** Exists a frame ready to use */
		if(bufDescTable[clockHand].valid == false){
			bufDescTable[clockHand].Clear();
			frame = clockHand;
			return;
		}
		// If has been referenced recently, changes value
		else if(bufDescTable[clockHand].refbit == true){
			bufDescTable[clockHand].refbit = false;
			continue;
		}
		// If there isnt 0 things pinned
		else if(bufDescTable[clockHand].pinCnt!=0){
			continue;
		}
		/** Found a location to allocate */
		else{
			if(bufDescTable[clockHand].dirty == true){
				bufDescTable[clockHand].file -> writePage(bufPool[clockHand]);
			}
			/** Remove the original page from BufDesc table */
			hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
			bufDescTable[clockHand].Clear();
			frame = clockHand;
		
			return;
		}
	}
	
	/** Throw exception if all buffer frames are pinned */
	throw BufferExceededException();
}

/**
 * Attempt to find and read page from buffer pool
 * @param file pointer to file object
 * @param pageNo page number to read
 * @param page pointer to page object
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page){
	FrameId frameNo;
	try{
		/** If page in buffer pool */
		hashTable->lookup(file,pageNo,frameNo);
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		page = &bufPool[frameNo];
	}catch(HashNotFoundException& e){

		/** If page not in buffer pool. Return pointer to frame containing the page */
		allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file,pageNo,frameNo);
		bufDescTable[frameNo].Set(file,pageNo);
		page = &bufPool[frameNo];
	}
}

/**
 * Unpin frame since it is no longer in use. Sets dirty bit if appropriate.
 * @param file pointer to file object
 * @param pageNo page number needed to decrement pin count
 * @param dirty dirty bit to indicate changes made
 * @throws PageNotPinnedException if pin count already is 0
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty){
	// Unpins a page if it exists in the hash table
	try{
		FrameId frameNo;
		hashTable->lookup(file,pageNo,frameNo);
		// If the page is not pinned throw exception
		if(bufDescTable[frameNo].pinCnt==0){
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}
		// Decreases the pincount
		if(bufDescTable[frameNo].pinCnt>0){
			bufDescTable[frameNo].pinCnt--;
		}
		// Sets dirty bit to true if told to by boolean argument
		if(dirty==true){
			bufDescTable[frameNo].dirty=true;
		}
	}catch(HashNotFoundException& e){
	}
}

/**
 * Allocates an empty page and calls allocBuf() to obtain a buffer pool frame
 * Sets up the frame that was just emptied
 * @param file pointer to file object
 * @param pageNo page number of newly alloocated page
 * @param page pointer to buffer frame allocated for the page
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page){
	/** allocate an empty page */
	Page np = file->allocatePage();
	/** obtain a buffer pool frame */
	FrameId frameNo;
	allocBuf(frameNo);
	/** return page number of newly allocated page via pageNo */
	pageNo = np.page_number();
	bufPool[frameNo] = np;
	/** insert into hashTable */
	hashTable->insert(file,pageNo,frameNo);
	bufDescTable[frameNo].Set(file,pageNo);
	
	/** pointer to the buffer frame */
	page = &bufPool[frameNo];
}

/**
 * Flush page to disk while scanning bufTable for pages belonging to file
 * @param file pointer to file object
 * @throws PagePinnedException if a page of the file is pinned
 * @throws BadBufferException if invalid page belonging to file is encountered
 */
void BufMgr::flushFile(const File* file){
	// Searches thorugh all the frames
	for(FrameId i=0;i<numBufs;i++){
		// If invalid
		if(bufDescTable[i].valid==false){
			throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, 
			bufDescTable[i].valid, bufDescTable[i].refbit);
		}
		// If pinned
		else if(bufDescTable[i].pinCnt>0){
			throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, 
			bufDescTable[i].frameNo);
		}
		// If still dirty
		else if(bufDescTable[i].dirty == true){
			bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
			bufDescTable[i].dirty = false;
		}
		// Removes page
		hashTable->remove(bufDescTable[i].file,bufDescTable[i].pageNo);
		bufDescTable[i].Clear();
	}
}

/**
 * Deletes page from file
 * Ensures frame is freed and entry in hash table is removed if in a frame
 * @param file pointer to file object
 * @param pageNo page number of page to be deleted
 */
void BufMgr::disposePage(File* file, const PageId PageNo){
	try{
			FrameId frameNo;
			/** Make sure that page to be deleted is allocated in buffer pool */
			hashTable->lookup(file,PageNo,frameNo);
			/** frame is freed */
			bufDescTable[frameNo].Clear();
			/** correspondingly entry from hash table is also removed */
			hashTable->remove(bufDescTable[frameNo].file,bufDescTable[frameNo].pageNo);
			/** delete from file */
			file->deletePage(PageNo);
	}catch(HashNotFoundException& e){
	}
}

/**
 * Print method displaying frame numbers and total number of valid frames
 * Used for testing purposes
 */
void BufMgr::printSelf(void) {
	BufDesc* tmpbuf;
	int validFrames = 0;
  
	for (std::uint32_t i = 0; i < numBufs; i++) {
  		tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true) {
   	 	validFrames++;
	}
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}
}
