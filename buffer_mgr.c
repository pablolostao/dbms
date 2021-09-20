#include "buffer_mgr.h"
#include "storage_mgr.h"

#include <stdlib.h>

int bumpToHead(IntList *il,ListEntry *le){//Append element at the tail and return it
	int a = 1, b = 1;
	if(le==(il->tail)) {
		a = 1;
	}
	if(le==(il->head)) {
		a = 2;
	}
	else {
		a = 3;
	}
	
	switch(a) {
		case 1: return le->index; break;//If the element is at the tail then return it
		case 2: il->head=il->head->nextpoint; break;//Reset the head while it is the head
		case 3: le->prevpoint->nextpoint=le->nextpoint; break;// set the next pointer of prevpoint
	}	
	do {
		le->nextpoint->prevpoint=le->prevpoint;
		break;
	}
	while(le->nextpoint);//Make sure it is not the last element
	switch(b){
		case 1:il->tail->nextpoint=le;
		case 2:le->prevpoint=il->tail;
		case 3:il->tail=le;
			b = 0;}
	return le->index;
}

RC initPFHandle(BM_PFHandle *pointer, const char *const pageFileName, int pagenumbers,ReplacementStrategy strategy, void *initData){
	if(pageFileName == NULL){
		RC_message="file not exist,please check your file";
		return RC_FILE_NOT_FOUND;
	}
	pointer->fh=malloc(sizeof(SM_FileHandle));	
	RC rc=openPageFile((char *)pageFileName,pointer->fh);
	while(rc!=RC_OK) {
		return rc;
	}
	pointer->pageFrame=malloc(pagenumbers*sizeof(BM_PageFrame));
	if(pointer == NULL){
		RC_message="file not exist,please check your file";
	}else{
	pointer->pMap=malloc(pagenumbers*sizeof(PageNumber));}
	
	
	long int i;		
	for(i=0;i<pagenumbers;i++){		
		pointer->pageFrame[i].pageNum=NO_PAGE;
	}
	int j;
	for(j=0;j<pagenumbers;j++){	
		pointer->pageFrame[j].dirty=false;
	}
	int k;
	for(k=0;k<pagenumbers;k++){		
		pointer->pageFrame[k].fixCount=0;
	}
	int l;
	for(l=0;l<pagenumbers;l++){		
		pointer->pMap[l]=-1;
	}
	
	int start = 1;
	switch(start){
		case 1:pointer->PmSize=pagenumbers;
		case 2:pointer->numReadIO=pointer->numWriteIO=0;
		case 3:pointer->rep=malloc(sizeof(BM_Replacement_Handle));
		case 4:pointer->rep->cur=0;
		case 5:pointer->rep->size=pagenumbers;
	}
	
	do{
		pointer->rep->initData=initData;
		break;
	}
	while(1 == 1);
	
	
	int b = 1;
	if(strategy==RS_FIFO){
		b = 1;
	}else if(strategy==RS_LRU){
		b = 2;
	}else if(strategy==RS_CLOCK){
		b = 3;
	}
	switch(b) {
		case 1: pointer->rep->dataStruct=NULL; break;
		case 2: if(1 == 1) pointer->rep->dataStruct=malloc(sizeof(LRU));
			LRU *lru=pointer->rep->dataStruct;
			lru->frameList=malloc(sizeof(IntList));//Initiate list
			if(1 == 1) lru->frameMap=malloc(pagenumbers*sizeof(ListEntry));//Initiate the map
			long int count;
			for(count=0;count<pagenumbers;count++){//Initiate both map and entries of linked list.
				ListEntry *le=(lru->frameMap)+count;
				int k = count;
				le->index=k;
				int c = 1;
				if(count==0){ 
					c = 1;
				}else {
					c = 2;
				}
				switch(c) {
					case 1: le->prevpoint=NULL;
						lru->frameList->head=le;//Make original list's head 0
						break;
					case 2: le->prevpoint=le-1; break;
				}
				int d = 1;
				if(count==pagenumbers-1){
					d = 1;
				} else {
					d = 2;
				}
				switch(d) {
					case 1: le->nextpoint=NULL;
						lru->frameList->tail=le;//The last element should be the tail
						break;
					case 2: le->nextpoint=le+1; break;
				}
			} break;
		case 3: pointer->rep->dataStruct=calloc(pagenumbers,sizeof(bool)); break;
	}
	printf("Initialie PFHandle scuueed!");
	return RC_OK;	
}

RC initBufferPool(BM_BufferPool *const pointer, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *initData){	
	char *pName = pointer->pageFile;
	pointer->pageFile=(char *)pageFileName;
	pName = pointer->pageFile;
	if(pName == NULL){
		RC_message="file not exist,please check your file";
		return RC_FILE_NOT_FOUND;
	}
	pointer->numPages=numPages;
	if(pName != NULL)
		pointer->strategy=strategy;
	
	BM_PFHandle *pfh=malloc(sizeof(BM_PFHandle));
	RC rc=initPFHandle(pfh, pageFileName, numPages, strategy, initData);//Initiate BM_PFHandle
	while(rc!=RC_OK) {
		return rc;
	}

	pointer->mgmtData=pfh;

	printf("Initialize bufferpool succeed!");
	return RC_OK;
}

RC closePFHandle(BM_PFHandle *pfh, ReplacementStrategy strategy){
	RC rc=closePageFile(pfh->fh);
	while(rc!=RC_OK) {
		return rc;
	}
	int aa;
	if(strategy==RS_LRU){
		aa = 1;
	}else if(strategy==RS_CLOCK){
		aa = 4;
	}
	switch(aa){
		case 1:free(((LRU *)pfh->rep->dataStruct)->frameList);
		case 2:free(((LRU *)pfh->rep->dataStruct)->frameMap);
		case 3:free(pfh->rep->dataStruct); break;
		case 4: free(pfh->rep->dataStruct); break;
	}	
	int start = 1;
	switch(start){
		case 1:free(pfh->rep);
		case 2:free(pfh->fh);
		case 3:free(pfh->pageFrame);
		case 4:free(pfh->pMap);
		case 5:free(pfh);
	}

	printf(" ");
	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const pointer){
	RC rc=forceFlushPool(pointer);//Before shut down, write disk with dirty frame
	while(rc!=RC_OK) {
		return rc;
	}
	if(pointer == NULL){
		RC_message="file not exist,please check your file";
	}else {
		rc=closePFHandle(pointer->mgmtData, pointer->strategy);
		while(rc!=RC_OK) {
			return rc;
		}
	}

	RC_message="Bufferpool shutdown";
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
	long int count=0;
	BM_PFHandle *pointer=bm->mgmtData;
	BM_PageFrame *pagefile;
	
	int fixnum = 0;
	while(count<bm->numPages){
		pagefile=pointer->pageFrame+count;
		if(pagefile->dirty==true && pagefile->fixCount==fixnum){
			while(writeBlock(pagefile->pageNum,pointer->fh,pagefile->data)!=RC_OK) {
				return RC_WRITE_FAILED;
			}
			pagefile->dirty=false;
			pointer->numWriteIO++;		
		}
		count = count + 1;
	}
	RC_message="Successfully proceed function Force flush pool";
	printf(" ");
	return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	char *pName = bm->pageFile;
	if (pName == NULL)
	{
		RC_message = "file not exist,please check your file";
		return RC_FILE_NOT_FOUND;
	}
	BM_PFHandle *bookkeeping_info = bm->mgmtData;
	int pagenum = page->pageNum;
	(bookkeeping_info->pMap[pagenum] + bookkeeping_info->pageFrame)->dirty = true;
	RC_message = "file not exist,please check your file";
	return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	char *pName = bm->pageFile;
	if (pName == NULL)
	{
		RC_message = "file not exist,please check your file";
		return RC_FILE_NOT_FOUND;
	}

	BM_PFHandle *pfile_handle = bm->mgmtData;
	int pagenum = page->pageNum;
	if ((pfile_handle->pMap[pagenum]) == -1)
	{
		RC_message = "No page be pinned.";
		return RC_OK;
	}
	BM_PageFrame *pointf = pfile_handle->pageFrame + pfile_handle->pMap[pagenum];
	int user_count = pointf->fixCount;
	if (user_count > 0)
	{
		//pinpage has occured,to unpinpage,we can reduce the the numbers of users by one
		pointf->fixCount--;
		//reset the page handel
		page->data = NULL;
	}
	RC_message = "unpinPage successful";
	return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	char *pName = bm->pageFile;
	if (pName == NULL)
	{
		RC_message = "file not exist,please check your file";
		return RC_FILE_NOT_FOUND;
	}
	BM_PFHandle *pfile_handle = bm->mgmtData;
	int pagenum = page->pageNum;
	BM_PageFrame *pointf = pfile_handle->pageFrame + pfile_handle->pMap[pagenum];
	RC wB = writeBlock(pagenum, pfile_handle->fh, pointf->data);
	if (pointf->dirty == true)
	{
		if (wB != RC_OK)
		{
			return wB;
		}

		pfile_handle->numWriteIO += pfile_handle->numWriteIO;
		pointf->dirty = false;
	}

	RC_message = "forcePage successful";
	return RC_OK;
}

int InitMapCache(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	int pagenum = page->pageNum;
	BM_PFHandle *pfile_handle = bm->mgmtData;
	while (pfile_handle->PmSize <= pagenum)
	{
		RC_message = "the capacity of map is less than pagenum";
		int *Old_pMSize = pfile_handle->pMap;
		int pMSize = pfile_handle->PmSize;
		int extend_pMSize = 2 * pMSize;
		pfile_handle->pMap = realloc(Old_pMSize, extend_pMSize * sizeof(PageNumber));
		for (int i = pMSize; i < extend_pMSize; i++)
		{
			if (pfile_handle->pMap[i] == -1)
			{
				continue; //pMap has been initialized
			}
			else
			{
				pfile_handle->pMap[i] = -1;
			}

		} //each time extend the size to twice as large so the amortized time is O(1)
		pfile_handle->PmSize = pfile_handle->PmSize * 2;
	}

	return pfile_handle->pMap[pageNum];
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
		   const PageNumber pageNum)
{
	char *pName = bm->pageFile;

	if (pName == NULL)
	{
		RC_message = "file not exist,please check your file";
		return RC_FILE_NOT_FOUND;
	}

	if (bm == NULL)
	{
		RC_message = "Bufferpool is not Initialize";
		return RC_BUFFERPOOL_NOT_INIT;
	}

	page->pageNum = pageNum;
	BM_PFHandle *pfile_handle = bm->mgmtData;
	if (ensureCapacity(pageNum + 1, pfile_handle->fh) != RC_OK)
	{
		RC_message = "Capacity is insufficient";
		return RC_CAPACITY_NOT_ENOUGH;
	}

	int curPIndex = InitMapCache(bm, page, pageNum);

	//int curPIndex=pfile_handle->pMap[pageNum];
	if (curPIndex == -1)
	{
		int fCount = 0;
		int iCache = -1;
		bool isDirty = false;
		curPIndex = pfile_handle->pMap[pageNum] = replace(bm);
		PageNumber oldPage = pfile_handle->pageFrame[curPIndex].pageNum;
		if (oldPage != NO_PAGE && !isDirty)
		{
			pfile_handle->pMap[oldPage] = iCache;
		}
		BM_PageFrame *pFrame = pfile_handle->pageFrame;
		if (!isDirty)
		{
			pFrame[curPIndex].dirty = false;
			pFrame[curPIndex].fixCount = fCount;
			pFrame[curPIndex].pageNum = pageNum;
			page->data = pfile_handle->pageFrame[curPIndex].data;
		}
		if (readBlock(pageNum, pfile_handle->fh, page->data) != RC_OK)
		{
			return RC_FILE_NOT_FOUND;
		}
		pfile_handle->pageFrame[curPIndex].fixCount++;
		pfile_handle->numReadIO++;
	}
	else if (curPIndex != -1)
	{
		bool Pinned = true;
		int index = 0;
		if (Pinned && page->data != &(pfile_handle->pageFrame[curPIndex].data[index]))
		{
			pfile_handle->pageFrame[curPIndex].fixCount++;
			page->data = pfile_handle->pageFrame[curPIndex].data;
		}
		switch (bm->strategy)
		{
		case RS_LRU:
		{
			LRU *lru = (LRU *)pfile_handle->rep->dataStruct;
			bumpToHead(lru->frameList, lru->frameMap + curPIndex);
			break;
		}
		case RS_CLOCK:
		{
			((bool *)pfile_handle->rep->dataStruct)[curPIndex] = 1;
			break;
		}
		case RS_LRU_K:
			break;
		case RS_LFU:
			break;
		case RS_FIFO:
			break;
		}
	}

	RC_message = "Page pinned";
	return RC_OK;
}

int replace(BM_BufferPool *bm)
{
	BM_PFHandle *pfh = bm->mgmtData;
	int ret; //Output of this funtion
	int result;
	int a;
	if (bm->strategy == RS_FIFO){
		a = 1;
	}else if (bm->strategy == RS_LRU){
		a = 2;
	}else if (bm->strategy == RS_CLOCK){
		a = 3;
	}
	switch(a){
		case 1:
			while (pfh->pageFrame[pfh->rep->cur].fixCount != 0)//Get the place with fixCount that is zero
		{ 
			BM_PFHandle *aca = pfh;
			aca->rep->cur =(pfh->rep->cur + 1) % pfh->rep->size;
			pfh->rep->cur = aca->rep->cur;
		}
		BM_PageFrame *pointer1 = pfh->pageFrame + pfh->rep->cur;
		BM_PageFrame *pf = pointer1;
		
		if (pf->dirty == true)//writedisk while frame is dirty
		{ 
			RC rc = writeBlock(pf->pageNum, pfh->fh, pf->data);
			while (rc != RC_OK)
				return rc;
			pfh->numWriteIO++; 
		}
		int b = pfh->rep->cur;
		ret = b;
		if(1 == 1){
			pfh->rep->cur = (pfh->rep->cur + 1) % pfh->rep->size;
		}
		break;
		case 2:{
			LRU *pot = (LRU *)pfh->rep->dataStruct;
			LRU *pt = pot;
			LRU *pit = pt;
			LRU *lru = pit;
			ListEntry *LIE = lru->frameList->head;
			ListEntry *le = LIE;
			while (pfh->pageFrame[le->index].fixCount != 0)
			{
				le = le->nextpoint;
			}
			result = bumpToHead(lru->frameList, le);
			ret = result;
			break;}
		case 3:
			{bool *clock = pfh->rep->dataStruct;
			long int time;
			
			while (pfh->pageFrame[pfh->rep->cur].fixCount != 0 || clock[pfh->rep->cur] == true)
			{	int cursize = pfh->rep->cur;
				time = 0;						  
				clock[cursize] = time; //make sure we can tell which is not recently used
				 cursize = (cursize + 1) % pfh->rep->size;
			}
			BM_PageFrame *pce = pfh->pageFrame + pfh->rep->cur;
			BM_PageFrame *pf = pce;
			if (pf->dirty == true)//writedisk while frame is dirty
			{ 
				RC rc = writeBlock(pf->pageNum, pfh->fh, pf->data);
				while (rc != RC_OK)
					return rc;
				pfh->numWriteIO++; 
			}
			result = pfh->rep->cur;
			ret = result;
			int use = 1;
			clock[pfh->rep->cur] = use; //make sure we can tell which is recently used
			pfh->rep->cur = (result + 1) % pfh->rep->size;
			break;}
	}
	return ret;
}

//Returns an array of PageNumber(ints) which size is bm->numPages (number of frames of the buffer pool) and the value is the page id linked with that frame.
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	int *frameContents = malloc(sizeof(int) * bm->numPages);
	int i;
	for (i = 0; i < bm->numPages; i++)
	{
		frameContents[i] = bm->mgmtData->pageFrame[i].pageNum;
	}
	return frameContents;
}


//Returns an array of bools which size is bm->numPages (number of frames of the buffer pool) and the value is 1 if that frame is dirty or 0 if not.
bool *getDirtyFlags(BM_BufferPool *const bm)
{
	bool *dirtyFlags = malloc(sizeof(bool) * bm->numPages);
	int i;
	for (i = 0; i < bm->numPages; i++)
	{
		dirtyFlags[i] = bm->mgmtData->pageFrame[i].dirty;
	}
	return dirtyFlags;
}

//Returns an array of ints which size is bm->numPages (number of frames of the buffer pool) and the value is the number of 'threads' using that frame.
int *getFixCounts(BM_BufferPool *const bm)
{
	int *fixCounts = malloc(sizeof(int) * bm->numPages);
	int i;
	for (i = 0; i < bm->numPages; i++)
	{
		fixCounts[i] = bm->mgmtData->pageFrame[i].fixCount;
	}
	return fixCounts;
}

//Gives the total number of read operations
int getNumReadIO(BM_BufferPool *const bm)
{
	//It is directly stored in mgmtData, so return.
	return bm->mgmtData->numReadIO;
}


//Gives the total number of write operations
int getNumWriteIO(BM_BufferPool *const bm)
{
	//It is directly stored in mgmtData, so return.
	return bm->mgmtData->numWriteIO;
}
