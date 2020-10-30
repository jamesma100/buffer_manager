/**
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

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/*Flushes out all dirty pages and 
deallocates the buffer pool and the BufDesc table.
*/
BufMgr::~BufMgr() {
	for (FrameId i = 0; i < numBufs; i++) {
		if(bufDescTable[i].dirty) {
			flushFile(bufDescTable[i].file);
		}
	}
	bufPool = NULL;
	bufDescTable = NULL;

}

void BufMgr::advanceClock()
{
	clockHand = (clockHand+1)%numBufs;
}
/*
Allocates a free frame using the clock algorithm; 
if necessary, writing a dirty page back to disk. 
Throws BufferExceededException if all buffer frames are pinned. 

This private method will get called by the readPage() and allocPage() 
methods described below. Make sure that if the buffer frame allocated 
has a valid page in it, you remove the appropriate entry from the hash 
table.
*/
void BufMgr::allocBuf(FrameId & frame) 
{

for(FrameId i=0;i<numBufs;i++){
	advanceClock();
	//already exist
	if(bufDescTable[clockHand].valid == false){
		frame = clockHand;
		break;
	}
	else if(bufDescTable[clockHand].refbit == true){
		bufDescTable[clockHand].refbit = false;
		continue;
	}
	else if(bufDescTable[clockHand].pinCnt!=0){
		continue;
	}
	//found location to allocate
	else{
		//remove the original page
		hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
		frame = clockHand;
		if(bufDescTable[clockHand].dirty == true){
			bufDescTable[clockHand].file -> writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;
			break;
		}
	}
}
//all buffer frames are pinned
throw BufferExceededException();
}

/*
First check whether the page is already in the buffer pool by invoking 
the lookup() method, which may throw HashNotFoundException when page is 
not in the buffer pool, on the hashtable to get a frame number. 
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try{
		/*
		Case 2: Page is in the buffer pool. In this case set the appropriate 
		refbit, increment the pinCnt for the page, and then return a pointer 
		to the frame containing the page via the page parameter.
		*/
		hashTable->lookup(file,pageNo,frameNo);
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		page = &bufPool[frameNo];
	}catch(HashNotFoundException& e){
		/*
		Case 1: Page is not in the buffer pool. Call allocBuf() to allocate a 
		buffer frame and then call the method file->readPage() to read the page 
		from disk into the buffer pool frame. Next, insert the page into the 
		hashtable. Finally, invoke Set() on the frame to set it up properly. 
		Set() will leave the pinCnt for the page set to 1. Return a pointer to 
		the frame containing the page via the page parameter.
		*/
		allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file,pageNo,frameNo);
	}
}

/*
Decrements the pinCnt of the frame containing (file, PageNo) and, 
if dirty == true, sets the dirty bit. Throws PAGENOTPINNED if the pin count 
is already 0. Does nothing if page is not found in the hash table lookup.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try{
		hashTable->lookup(file,pageNo,frameNo);
		if(bufDescTable[frameNo].pinCnt>0){
			bufDescTable[frameNo].pinCnt--;
		}
		if(bufDescTable[frameNo].dirty==true){
			bufDescTable[frameNo].dirty=false;
		}
	} catch(PageNotPinnedException& e){

	}
}
/*
The first step in this method is to to allocate an empty page in the specified 
file by invoking the file->allocatePage() method. This method will return a 
newly allocated page. Then allocBuf() is called to obtain a buffer pool frame.
Next, an entry is inserted into the hash table and Set() is invoked on the 
frame to set it up properly. The method returns both the page number of the 
newly allocated page to the caller via the pageNo parameter and a pointer to 
the buffer frame allocated for the page via the page parameter.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	//allocate an empty page
	Page np = file->allocatePage();
	//obtain a buffer pool frame
	FrameId frameNo;
	allocBuf(frameNo);
	//return page number of newly allocated page via pageNo
	pageNo = np.page_number();
	//insert into hashTable
	hashTable->insert(file,pageNo,frameNo);
	//Set() is invoked
	bufDescTable[frameNo].Set(file,pageNo);
	//pointer to the buffer frame
	page = &bufPool[frameNo];


}
/*
Should scan bufTable for pages belonging to the file. 
For each page encountered it should: (a) if the page is dirty, 
call file->writePage() to flush the page to disk and then set the 
dirty bit for the page to false, (b) remove the page from the hashtable 
(whether the page is clean or dirty) and (c) invoke the Clear() method 
of BufDesc for the page frame.
Throws PagePinnedException if some page of the file is pinned. 
Throws BadBufferException if an invalid page belonging to the file is 
encountered.

*/
void BufMgr::flushFile(const File* file) 
{
	for(FrameId i=0;i<numBufs;i++){
		if(bufDescTable[i].valid==false){
			throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, 
			bufDescTable[i].valid, bufDescTable[i].refbit);
		}
		else if(bufDescTable[i].pinCnt>0){
			throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, 
			bufDescTable[i].frameNo);
		}
		else if(bufDescTable[i].dirty == true){
			bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
			bufDescTable[i].dirty = false;
		}
		hashTable->remove(bufDescTable[i].file,bufDescTable[i].pageNo);
		bufDescTable[i].Clear();
	}
}

/*
This method deletes a particular page from file. Before deleting the page 
from file, it makes sure that if the page to be deleted is allocated a frame 
in the buffer pool, that frame is freed and correspondingly entry from hash 
table is also removed.
*/
void BufMgr::disposePage(File* file, const PageId PageNo)
{
	try{
			FrameId frameNo;
			//make sure that page to be deleted is allocated in buffer pool
			hashTable->lookup(file,PageNo,frameNo);
			//frame is freed
			bufDescTable[frameNo].Clear();
			//correspondingly entry from hash table is also removed
			hashTable->remove(bufDescTable[frameNo].file,bufDescTable[frameNo].pageNo);
			//delete from file
			file->deletePage(PageNo);
	}catch(HashNotFoundException& e){

	}
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
