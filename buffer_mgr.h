#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"
#include "storage_mgr.h"

// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
  RS_FIFO = 0,
  RS_LRU = 1,
  RS_CLOCK = 2,
  RS_LFU = 3,
  RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_Replacement_Handle{//structure for replacement handle
	int cur,size;
	void *initData;//get initialized data from initBufferPool
	void *dataStruct;//structure to help with the strategy
}BM_Replacement_Handle;

typedef struct BM_PageFrame{//structure for pageframe 
	PageNumber pageNum;//the corresponding page number in the file
	bool dirty;//mark if the content is modified
	int fixCount;//mark how many users are using this page
	char data[PAGE_SIZE];//page content
}BM_PageFrame;


typedef struct BM_PFHandle{//structure to handle pageframes
	SM_FileHandle *fh;//point to the file handle
	BM_Replacement_Handle *rep;// point to manger replacement
	BM_PageFrame *pageFrame;//array of pageframes
	int *pMap;// Array which mapping the page number to pageframe number
	int PmSize;//the size of pageMap
	int numReadIO,numWriteIO;//track number of IOs
}BM_PFHandle;

typedef struct BM_BufferPool {
  char *pageFile;
  int numPages;
  ReplacementStrategy strategy;
  BM_PFHandle *mgmtData; // use this one to store the bookkeeping info your buffer 
                  // manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
  PageNumber pageNum;
  char *data;
} BM_PageHandle;



typedef struct ListEntry{
	int index;//the pageframe index
	struct ListEntry *prevpoint;
	struct ListEntry *nextpoint;
}ListEntry;

typedef struct IntList{
	ListEntry *head,*tail;//A Queue that store Integer, dequeue from the head of list, enqueue from the tail of list
}IntList;

typedef struct LRU{
	IntList *frameList;//a list of page frame numbers
	ListEntry *frameMap;//map from page frame number to ListEntry
}LRU;

int bumpToHead(IntList *il,ListEntry *le);//bump a certain element up to head

// convenience macros
#define MAKE_POOL()					\
  ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
  ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initPFHandle(BM_PFHandle *pfh, const char *const pageFileName, int numPages,
		ReplacementStrategy strategy, void *initData);
RC closePFHandle(BM_PFHandle *pfh, ReplacementStrategy strategy);
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy, 
		  void *initData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
int InitMapCache(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum);

// Statistics Interface
int replace(BM_BufferPool *bm);
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

#endif
