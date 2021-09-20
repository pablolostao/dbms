#include "storage_mgr.h"
#include "malloc.h"
#include "string.h"
#include <sys/stat.h>
#include <errno.h>

extern int errno;
FILE *fp;

//Initalize the storage manager
void initStorageManager(void)
{
    printf("Storage manager initialized!\n");
}

/********************File Related Methods*******************/


//Create an empty one-page file, if there is a file with that name, it is overwritten.
RC createPageFile(char* fileName)
{
	FILE *fp;
    char buffer[PAGE_SIZE] = {0};
	fp = fopen(fileName, "wb");
    if (fp != NULL)
    {
        int eltsSuccessfullyWritten = fwrite(buffer, sizeof(char), sizeof(buffer), fp);
        if (eltsSuccessfullyWritten == PAGE_SIZE)
        {
            fclose(fp);
            printf("createPageFile - RC_OK\n");
            return RC_OK;
        }
    }
    fclose(fp);
    printf("createPageFile - RC_WRITE_FAILED\n");
    return RC_WRITE_FAILED;
}


//Opens a file, set SM_FileHandle with num of total pages.
RC openPageFile(char* fileName, SM_FileHandle* fHandle)
{
    FILE *file;
	file = fopen(fileName, "rb+");
    if (file != NULL)
    {
        fseek(file, 0, SEEK_END);             //Go to the end of the file
        int nPages = ftell(file) / PAGE_SIZE; // get nPages
        fseek(file, 0, SEEK_SET);             //Go again to the beginning
        fHandle->curPagePos = 0;
        fHandle->fileName = fileName;
        fHandle->totalNumPages = nPages;
        fHandle->mgmtInfo = file;
        printf("openPageFile - RC_OK\n");
        return RC_OK;
    }
    printf("openPageFile - RC_FILE_NOT_FOUND\n");
    return RC_FILE_NOT_FOUND;
}

/*
Close an open page file or destroy (delete) a page file.
*/
RC closePageFile(SM_FileHandle *fHandle)
{
    if (fHandle == NULL)
    {
        printf("closePageFile - RC_FILE_HANDLE_NOT_INIT\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    fHandle->curPagePos = -1;
    fHandle->fileName = NULL;
    fHandle->totalNumPages = -1;
    fHandle->mgmtInfo = NULL;
    printf("closePageFile - RC_OK\n");
    return RC_OK;
}

//It deletes the file unless it does not exist.
RC destroyPageFile(char *fileName)
{
    int del = remove(fileName);
    if (!del)
    {
        printf("destroyPageFile - RC_OK\n");
        return RC_OK;
    }
    printf("destroyPageFile - RC_FILE_NOT_FOUND\n");
    return RC_FILE_NOT_FOUND;
}


/*******************Read and Write Methods************************/


RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

	if(pageNum > fHandle->totalNumPages && pageNum < 0 ){		//Check if the page number is valid, its invalid if page number > Total number of pages

		return RC_READ_NON_EXISTING_PAGE;			//Return error message on invalid page
	}

	if(1 == 1)
		fp = fopen(fHandle->fileName,"r");
	int a = 1;
	if(fp == NULL)
		a = 1;
	else if(fseek(fp, pageNum*PAGE_SIZE,SEEK_SET) != 0)
		a = 2;
	else
		a = 3;

	switch(a){
		case 1:
			return RC_FILE_NOT_FOUND;

		case 2:
			return RC_FILE_NOT_FOUND;
		case 3:
			fread(memPage,sizeof(char),PAGE_SIZE,fp);
		fHandle->curPagePos = pageNum;
		fclose(fp);
		return RC_OK;}	        //Return successful reading of Block
    return 1;
}




//Return the current page position in a file
int getBlockPos(SM_FileHandle *fHandle){
	int a = 1;
	if (fHandle == NULL)
		if (1 == 1)
			a = 1;
	if (fHandle->mgmtInfo == NULL)
		if (1 == 1)
			a = 2;

	switch(a){
		case 1:				// can not find fHandle
			printError(RC_FILE_HANDLE_NOT_INIT);
			printf("fHandle is Null in getBlockPos()");
			return RC_FILE_HANDLE_NOT_INIT;

		case 2:				//CAN noT FIND FILE
			printError(RC_FILE_NOT_FOUND);
			printf("file is Null in getBlockPos()");
			return RC_FILE_NOT_FOUND;
            }

    return fHandle->curPagePos;
}





/*
The curPagePos should be moved to the page that was read.
If the user tries to read a block before the first page or after the last page of the file,
the method should return RC_READ_NON_EXISTING_PAGE.
*/

RC readFirstBlock(SM_FileHandle *fHandle , SM_PageHandle memPage){
	int a = 1;
	if (a == 1)
		fp = fopen(fHandle->fileName,"r");
	int b = 1;
	if (fp == NULL)
		b = 1;
	else
		b = 2;
	switch(b) {
		case 1:
			return RC_FILE_NOT_FOUND;
		case 2:
			readBlock(0,fHandle,memPage);	//Read file
			return RC_OK;	}	//Return
    return 1;
}

RC readPreviousBlock(SM_FileHandle *fHandle , SM_PageHandle memPage){
	do{
        fp = fopen(fHandle->fileName,"r");
        break;
        }while (1 == 1);

    int a = 1;
    if(fp == NULL)
        a = 1;
	else if(fHandle->curPagePos <= PAGE_SIZE)
        a = 2;

    switch(a){
        case 1: return RC_FILE_NOT_FOUND;
        case 2: return RC_READ_NON_EXISTING_PAGE;
        }
    if (1 == 1) {				//Point file handler's current position to previous block
        readBlock(getBlockPos(fHandle) - 1,fHandle,memPage);			//Read previous block
    }
	return RC_OK;						//return successful read from previous block
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (1 == 1) {
        do {
            int cur_page = getBlockPos(fHandle);
            readBlock(cur_page,fHandle,memPage);
            break;
            }while (1 == 1);		//Read contents of the currrent block
    }
	return RC_OK;				//return successful read from current block
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	do{
        return RC_FILE_NOT_FOUND;
        }while (fp == NULL );
    do {
        return RC_READ_NON_EXISTING_PAGE;

    }while (fHandle->curPagePos == PAGE_SIZE);

    int next_page = getBlockPos(fHandle) + 1;   //Increment the file handler's current position to point to the next block

	readBlock(next_page, fHandle, memPage);			//Read contents of the next block

	fclose(fp);
	return RC_OK;						//return successful read from next block
}


RC readLastBlock(SM_FileHandle *fHandle , SM_PageHandle memPage){
	fp = fopen(fHandle->fileName,"r");
	int totalPage = fHandle->totalNumPages;
	int a = 0;
	if(fp == NULL)
        a = 1;
	else if(fHandle->mgmtInfo == NULL)
        a = 2;
    else
        a = 3;
    switch(a) {
        case 1: return RC_FILE_NOT_FOUND;
        case 2: return RC_FILE_NOT_FOUND;
        case 3: readBlock(totalPage,fHandle,memPage);
    }
	fclose(fp);
	return RC_OK;
}


//Write a page to disk using either the current position or an absolute position.
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	//check the state of fHandle , pageNum and file
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
		RC_message = "file handle is missing, please check file handle";
	}else if(pageNum < 0){
        RC_message = "page is not found";
		return RC_READ_NON_EXISTING_PAGE;
	}else if(fHandle->totalNumPages < pageNum){
		RC_message = "Page number is out of range";
		return RC_READ_NON_EXISTING_PAGE;
	}else if(fHandle->fileName == NULL){
		RC_message = "file is not found, please check your file";
		return RC_FILE_NOT_FOUND;
	}
   // if fHandle is initialized and the pageNum is exist, move the pointer to the end of current page
   FILE *filePointer;
   filePointer = fHandle->mgmtInfo;
   int SetPointer = fseek(filePointer,pageNum*PAGE_SIZE,SEEK_SET);
   if(SetPointer != 0){
   	RC_message = "Fail to set the pointer to the end of current page";
   	return RC_FILE_NOT_FOUND;
   }else {  //set the position of pointer successfully, now write the block
   		int numwritten = fwrite(memPage,1,PAGE_SIZE,filePointer);
   		if(numwritten == PAGE_SIZE){
   			RC_message = "writeBlock successfully";
   			return RC_OK;
   		}else{
   			RC_message = "Fail to writeBlock";
   			return RC_WRITE_FAILED;
   		}
   }
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
   int pageNum = getBlockPos(fHandle);
   return writeBlock(pageNum,fHandle,memPage);
}


RC appendEmptyBlock (SM_FileHandle *fHandle){
if(fHandle == NULL){
      return RC_FILE_HANDLE_NOT_INIT;
      RC_message = "file handle is missing";
   }else if(fHandle->fileName == NULL){
      RC_message = "file is not found, please check your file";
      return RC_FILE_NOT_FOUND;
   }
   FILE *filePointer;
   filePointer = fHandle->mgmtInfo;
   int SetPointer = fseek(filePointer,0,SEEK_END);
   if(SetPointer != 0){
      RC_message = "Fail to set the pointer to the end of current page";
      return RC_FILE_NOT_FOUND;
   }else {
      char* Emptyblock;
      Emptyblock = (char*)calloc(PAGE_SIZE, sizeof(char));//Allocates the required memory space
      memset(Emptyblock, 0, PAGE_SIZE);//put 0 to block
      int numwritten = fwrite(Emptyblock,1,PAGE_SIZE,filePointer);
      if(numwritten == PAGE_SIZE){
            RC_message = "append Empty Block successfully";
            return RC_OK;
         }else{
            RC_message = "Fail to append Empty Block";
            return RC_WRITE_FAILED;
         }
   }
}


RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){

   FILE *filePointer;
   filePointer = fHandle->mgmtInfo;
   if(fHandle == NULL){
      return RC_FILE_HANDLE_NOT_INIT;
      RC_message = "file handle is missing";
   }else if(filePointer == NULL){
      RC_message = "file is not found, please check your file";
      return RC_FILE_NOT_FOUND;
   }
   int totalPages = fHandle->totalNumPages;
   if(totalPages - numberOfPages >= 0){
      RC_message = "Capacity ensured";
      return RC_OK;
   }else{
      for(int num = totalPages;num < numberOfPages;num++){
         appendEmptyBlock(fHandle);
      }

   }
   RC_message = "Capacity ensured";
      return RC_OK;

}
