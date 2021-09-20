#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include <string.h>
#include <stdlib.h>

int MaxRecordSize = 0;
int RecordSpace[10000];
// table and manager
RC initRecordManager (void *mgmtData){
	if (mgmtData==NULL){
		MaxRecordSize=1024;

	}else{
		MaxRecordSize = *(int *)mgmtData;
		for(int i = 0; i<10000;i++){
			RecordSpace[i] = -1;
		}
	}
	return RC_OK;
}

RC shutdownRecordManager (){
		for(int i = 0; i<10000;i++){
			RecordSpace[i] = -1;
		}
	return RC_OK;
}


RC createTable (char *name, Schema *schema){
	//createPageFile
	if(createPageFile(name) != RC_OK){
		return RC_WRITE_FAILED;
	}
	//openFile
	SM_FileHandle* fh = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
	if(openPageFile(name, fh) != RC_OK){
		return RC_FILE_NOT_FOUND;
	}

	char Fbuffer[PAGE_SIZE];
	char empty = {'\0'};
	if(Fbuffer != NULL){
		Fbuffer[PAGE_SIZE] = empty;
	}
	char *curr_point = Fbuffer;
	*(int *)curr_point=1;
	int j;
	curr_point = curr_point + sizeof(j);
	int NumOfAttr = schema->numAttr;
	*(int *)curr_point = NumOfAttr;
	curr_point = curr_point + sizeof(j);

	for(int i=0;i<NumOfAttr;i++){
		char *aNames = schema->attrNames[i];
		strcpy(curr_point,aNames);
		int NameLen = strlen(aNames);
		curr_point = curr_point + NameLen + 1;
	}


	for(int i=0;i<NumOfAttr;i++){
		*(DataType *)curr_point=schema->dataTypes[i];
		curr_point = curr_point + sizeof(DataType);
	}


	for(int i=0;i<NumOfAttr;i++){
		*(int *)curr_point=schema->typeLength[i];
		curr_point = curr_point + sizeof(i);
	}

	int key_Size = schema->keySize;
	*(int *)curr_point= key_Size;//copy key size
	curr_point=curr_point + sizeof(int);

	for(int i=0;i<(key_Size);i++){
		*(int *)curr_point=schema->keyAttrs[i];//copy keyAttrs
		curr_point=curr_point + sizeof(i);
	}

	int start = 0;
	*(int *)curr_point = start;
	curr_point=curr_point + sizeof(int);

	int RS = schema->recordSize;
	*(int *)curr_point=RS;//recording the size of one tuple

	if((writeBlock(0,fh,(SM_PageHandle)Fbuffer))!=RC_OK){
		return RC_WRITE_FAILED;
		RC_message="writeBlock FAILED";
}
		

	if((closePageFile(fh))!=RC_OK){
		RC_message = ("closePageFile failed");
		return RC_WRITE_FAILED;
}
		
	free(fh);//free the malloc
	RC_message="Table created";
	return RC_OK;
}
//openTable
RC openTable (RM_TableData *rel, char *name){
	BM_BufferPool *bm = MAKE_POOL();
	SM_FileHandle* fh = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
	if(openPageFile(name, fh) != RC_OK){
		return RC_FILE_NOT_FOUND;
	}
	else if(initBufferPool(bm, name, MaxRecordSize, RS_CLOCK, NULL) != RC_OK){
    	return RC_FILE_NOT_FOUND;
    }
	rel->mgmtData=bm;
	BM_PageHandle *ph = MAKE_PAGE_HANDLE();
	if (pinPage(bm, ph, 0) != RC_OK){
		RC_message="pinPage failed";
		return -1;
	}
	rel->name=name;
	rel->schema=malloc(sizeof(Schema));
	char *curr_point = ph->data;
	int j;
	curr_point = curr_point + sizeof(j);
	int NumOfAttr = rel->schema->numAttr=*(int *)curr_point;

	curr_point = curr_point + sizeof(j);
	char **attrNames = rel->schema->attrNames=malloc(NumOfAttr*sizeof(char *));

	for(int i=0; i<NumOfAttr; i++){
		attrNames[i]=curr_point;//set attrNames
		int NL = strlen(curr_point);
		curr_point=curr_point + NL + 1;
	}

	Schema *schema = rel->schema;

	schema->dataTypes=(DataType *)curr_point;//set data type
	curr_point = curr_point + NumOfAttr*sizeof(DataType);//cptr now point to beginning of type length

	int k;
	schema->typeLength=(int *)curr_point;//set type length
	curr_point = curr_point + NumOfAttr*sizeof(k);//cptr now point to keySize

	schema->keySize=*(int *)curr_point;//set keySize
	curr_point= curr_point + sizeof(k);//cptr now point to beginning of keyAttr

	schema->keyAttrs=(int *)curr_point;//set keyAttrs
	curr_point= curr_point + schema->keySize*sizeof(k);

	rel->num_tuple=(int *)curr_point;//read num_tuples
	curr_point =curr_point + sizeof(k);

	schema->recordSize=*(int *)curr_point;//read record size

	free(ph);
	RC_message="openTable successfully";
	return RC_OK;

}


//closeTable
RC closeTable (RM_TableData *rel){

	Schema *schema = rel->schema;
	free(schema->attrNames);
	free(schema);

	if ((shutdownBufferPool((BM_BufferPool *)(rel->mgmtData)))!=RC_OK){
		RC_message="shutdownBufferPool failed";
		return -1;

}
		
	free(rel->mgmtData);

	RC_message="closeTable successfully";
	return RC_OK;
}


//deleteTable
RC deleteTable (char *name){
	
	return destroyPageFile(name);//remove the page file
	
}



int getNumTuples (RM_TableData *rel){
	int NumTuples = *(rel->num_tuple);
	return NumTuples;


}


// Use a table to store records
RC insertRecord (RM_TableData *rel, Record *record){
	RC rc;
	BM_PageHandle *head=MAKE_PAGE_HANDLE();	
	rc = pinPage((BM_BufferPool *)(rel->mgmtData),head,0);
	while (rc!=RC_OK)
		return rc;
	BM_PageHandle *cur=MAKE_PAGE_HANDLE();
	Schema *rs = rel->schema;
	long int recordSize=rs->recordSize;//store recordSIze

	long int freepage=*(int *)head->data;//        get the page number that has free space
	rc=pinPage((BM_BufferPool *)(rel->mgmtData),cur,freepage);
	if (rc!=RC_OK)//pin page freepage
		return rc;
	
	long int nextpage=*(int *)cur->data;//cur->data is the data pointer, nextpage is to get the next free page
	int pn = cur->pageNum;
	while(nextpage==0)//if it's pinPage new crate page
		
		nextpage=*(int *)cur->data=1+pn;

	char *cptr=cur->data+sizeof(int);

	long int freeslot=(*(int *)cptr);//get how many free solt are there
	cptr=cptr+sizeof(int);//move to next position

	(record->id).page=*(int *)head->data;
	(record->id).slot=freeslot;
	int a = PAGE_SIZE-2*sizeof(int);
	int b = recordSize+sizeof(bool);
	long int max_slot=a/b;

	bool *marker=(bool *)cptr;//set a slotmap pointer
	cptr=cptr+(a/b)*sizeof(bool);//move cptr to the next position of slotmap 
	char *count = cptr+freeslot*recordSize;
	cptr=count;
	memcpy(cptr,record->data,recordSize);
	//write page with a copy of rcord data
	marker[freeslot]=true;//mark that slot is filled

	long int slotmove=freeslot+1;//indicate next free slot
	while(slotmove<a/b && marker[slotmove]==true)//find next free slot
		slotmove=slotmove+1;
	int sub = *(int *)((cur->data)+sizeof(int));
	sub=slotmove;//set slotmove 
	*(int *)((cur->data)+sizeof(int)) = sub;

	if (slotmove==max_slot){//when page is full
		int next = *(int *)(head->data);
		next=nextpage;
		*(int *)(head->data) = next;
		*(int *)(cur->data)=0;
	}

	*(rel->num_tuple)=*(rel->num_tuple)+1;//increase tuple number
	int x = 1, y = 1;
	if ((rc=markDirty((BM_BufferPool *)(rel->mgmtData),head))!=RC_OK)
		x = 1;
	else if ((rc=markDirty((BM_BufferPool *)(rel->mgmtData),cur))!=RC_OK)
		x = 2;
	else	x = 3;
	switch(x) {
		case 1: return rc; break;
		case 2: return rc; 
		case 3: break;
	}
	
	switch(y){
		case 1:unpinPage((BM_BufferPool *)(rel->mgmtData),cur);//unpin cur page
		case 2:unpinPage((BM_BufferPool *)(rel->mgmtData),head);//unpin head page
		case 3:free(cur);//free the page handle
		case 4:free(head);//free the page handle
	}
	printf("Successfully insert record");
	return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id){
	BM_PageHandle *current=MAKE_PAGE_HANDLE();
	
	RC rc;
	rc=pinPage((BM_BufferPool *)(rel->mgmtData),current,id.page);
	if (rc!=RC_OK)
		return rc;

	char *cptr=current->data;
	cptr=current->data+sizeof(int);//point cptr to marker
	int s = sizeof(int);
	int *freeslot=(int *)cptr;
	cptr=cptr + s;
	bool *tell=(bool *)cptr;
	bool *marker=tell;//point to markers
	int a = 1;
	if (marker[id.slot]==false) a = 1;
	else a = 2;
	switch(a){
		case 1: printf("Record does not exist");
			return RC_RM_RID_NOT_EXIST;
			break;
		case 2: marker[id.slot]=false;//this marks that this slot does not contain record
			if(id.slot<*freeslot)//at this circumstance set new min freeslot
				*freeslot=id.slot;
	}
	
	int d = *(int *)(current->data);
	if(d==0){//if not in a free page
		BM_PageHandle *head=MAKE_PAGE_HANDLE();
		rc=pinPage((BM_BufferPool *)(rel->mgmtData),head,0);
		while (rc!=RC_OK)
			return rc;
		int hdata = *(int *)(head->data);
		*(int *)(current->data)=hdata;
		int ide = id.page;
		*(int *)(head->data)=ide;

		markDirty((BM_BufferPool *)(rel->mgmtData),head);
		//makedirty
		unpinPage((BM_BufferPool *)(rel->mgmtData),head);
		free(head);
	}

	*(rel->num_tuple)=*(rel->num_tuple)-1;//decrease tuple number
	markDirty((BM_BufferPool *)(rel->mgmtData),current);
	BM_PageHandle *pointer;
	pointer=MAKE_PAGE_HANDLE();
	unpinPage((BM_BufferPool *)(rel->mgmtData),current);
	if(pointer == NULL)
		printf("null");
	free(current);
	RC_message="Sucessfully delete record";
	return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record){
	BM_PageHandle *current=MAKE_PAGE_HANDLE();

	RC rc;
	rc=pinPage((BM_BufferPool *)(rel->mgmtData),current,(record->id).page);
	if (rc!=RC_OK)
		return rc;
	char *cptr = current->data;
	char *curdata=cptr;
	int s = sizeof(int);
	curdata=curdata+2*s;
	bool *marker=(bool *)curdata;
	if(marker[record->id.slot]==false){//if there is no record
		printf("There is no record");
		return RC_RM_RID_NOT_EXIST;
	}
	Schema *schema = rel->schema;
	int recordSize=schema->recordSize;
	int sb = sizeof(bool);
	int ps = PAGE_SIZE-2*s;
	int rs = recordSize+sb;
	int max_slot=ps/rs;
	int ms = max_slot*sb;
	curdata=curdata+ms;
	int rr=recordSize*record->id.slot;
	curdata=curdata+rr;
	int sizc = sizeof(char);
	sizc++;
	memcpy(curdata,record->data,recordSize);
	markDirty((BM_BufferPool *)(rel->mgmtData),current);
	BM_PageHandle *pointer;
	pointer=MAKE_PAGE_HANDLE();
	unpinPage((BM_BufferPool *)(rel->mgmtData),current);
	if(pointer == NULL)
		printf("null");
	free(current);

	RC_message="Successfullt update record";
	return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record){
	
	BM_PageHandle *cur=MAKE_PAGE_HANDLE();//make page handle

	RC rc;
	rc=pinPage((BM_BufferPool *)(rel->mgmtData),cur,id.page);
	if (rc!=RC_OK)
		return rc;
	record->id=id;
	char *curdata=cur->data;//set to pointer to maek the position of data
	int s = sizeof(int);
	curdata+=2*s;//move the pointer curdata to the position of slot map

	bool *marker=(bool *)curdata;//set a marker point to the position of sp
	BM_PageHandle *pointer;
	pointer=MAKE_PAGE_HANDLE();
	bool markered = false;
	if (marker[id.slot]==markered){
		RC_message="can not find record";
		return RC_RM_RID_NOT_EXIST;
	}

	int a = PAGE_SIZE-2*s;
	int sb = sizeof(bool);
	int recordSize=rel->schema->recordSize;
	int b = recordSize+sb;
	int max_slot=a/b;
	int ms=max_slot*sb;
	curdata=curdata+ms;
	int rid = recordSize*id.slot;
	curdata=curdata+rid;
	memcpy(record->data,curdata,recordSize);
	if(pointer == NULL)
		printf("null");
	unpinPage((BM_BufferPool *)(rel->mgmtData),cur);
	if(pointer == NULL)
		printf("null");
	free(cur);

	RC_message="Sucessfully fetch record";//return sucessed fetch record
	return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	if(scan == NULL){
		return -1;
	}
	scan->mgmtData = malloc(sizeof(RM_ScanMgmtData));
	((RM_ScanMgmtData *)(scan->mgmtData))->cond = cond;
	((RM_ScanMgmtData *)(scan->mgmtData))->pageOfNextToCheck = 1;
	((RM_ScanMgmtData *)(scan->mgmtData))->nextSlotToCheck = 0;
	((RM_ScanMgmtData *)(scan->mgmtData))->nTuplesChecked = 0;
	scan->rel = rel;
	return RC_OK;
}



RC next (RM_ScanHandle *scan, Record *record){


	int pagenum=((BM_PFHandle *)((BM_BufferPool *)scan->rel->mgmtData)->mgmtData)->fh->totalNumPages;
	//get total page number

	BM_PageHandle *cur=MAKE_PAGE_HANDLE();//make page handle
	int i,j;
	int slot=((RM_ScanMgmt *)(scan->mgmtData))->id.slot;
	int recordsize = scan->rel->schema->recordSize;
	int totalPage = (PAGE_SIZE-2*sizeof(int));
	int max_slot=totalPage/recordsize;
	int init = 0;
	int page=((RM_ScanMgmt *)(scan->mgmtData))->id.page;
	bool pin = true;
	Value *result;//holding evaluation result

	for (i=page;i<pagenum;i++){
		if (i==page) j=slot;
		else j=init;

		if ((pinPage((BM_BufferPool *)(scan->rel->mgmtData),cur,i))!=RC_OK)//pin target page
			return -1;

		bool *slotmap=(bool *)(cur->data+2*sizeof(int));//get slotmap
		while (j<max_slot){
			if (slotmap[j]==true&&pin){
				RID id={i,j};
				if(true){
getRecord(scan->rel,id,record);
				evalExpr(record, scan->rel->schema, ((RM_ScanMgmt *)(scan->mgmtData))->cond, &result);

}
bool res = result->dt==DT_BOOL && result->v.boolV;
				
				if(res==true){
			if (id.slot == scan->rel->slotsPerPage - 1)
				{
					((RM_ScanMgmtData *)(scan->mgmtData))->pageOfNextToCheck++;
					((RM_ScanMgmtData *)(scan->mgmtData))->nextSlotToCheck = 0;
				}
					freeVal(result);
					unpinPage((BM_BufferPool *)(scan->rel->mgmtData),cur);
					
					((RM_ScanMgmt *)(scan->mgmtData))->id.page=i;
		((RM_ScanMgmt *)(scan->mgmtData))->id.slot=j+1;//start from next slot next time
					free(cur);
					return RC_OK;
				}
				freeVal(result);
			}
			j++;
		}

		unpinPage((BM_BufferPool *)(scan->rel->mgmtData),cur);
	}
	if(pin){
	free(cur);
	((RM_ScanMgmt *)(scan->mgmtData))->id.page=pagenum;
}
	
	
	RC_message="no more tuples";
	return RC_RM_NO_MORE_TUPLES;
}


RC closeScan (RM_ScanHandle *scan){

	((RM_ScanMgmtData *)(scan->mgmtData))->pageOfNextToCheck = -1;
	((RM_ScanMgmtData *)(scan->mgmtData))->nextSlotToCheck = -1;
	((RM_ScanMgmtData *)(scan->mgmtData))->nTuplesChecked = -1;
	free(scan->mgmtData);
	return RC_OK;
}



int getRecordSize (Schema *schema){
	int recSize = schema->recordSize;
	return recSize;
}


 int increrec (int i, int attributenumber, int recordSize, DataType *dataTypes, int *Lengthofthetypedata) {
	for(i=0;i<attributenumber;i++){
		switch(dataTypes[i]){
		case DT_INT:
			recordSize+=sizeof(int);
			RC_message="INT case";
			break;
		case DT_FLOAT:
			recordSize+=sizeof(float);
			RC_message="FLOAT case";
			break;
		case DT_BOOL:
			recordSize+=sizeof(bool);
			RC_message="BOOL case";
			break;
		case DT_STRING:
			recordSize+=Lengthofthetypedata[i];
			break;
		}
	}
	return recordSize;
}

Schema *createSchema (int attributenumber, char **NameofAttribute, 
                 DataType *dataTypes, int *Lengthofthetypedata, int sizeofattributekey, int *attributekeys){

	Schema *schema=malloc(sizeof(Schema));
	if(schema == NULL) {
		printf("Schema is empty");
	}
	schema->typeLength=Lengthofthetypedata;
	schema->dataTypes=dataTypes;
	schema->attrNames=NameofAttribute;//Distribute values to attributes	
	schema->numAttr=attributenumber;//set numarr from schema to inout attributenember	
	schema->keyAttrs=attributekeys;//set keyAttrs to input attributekeys
	schema->keySize=sizeofattributekey;
	
	
	long int recordSize=0;
	long int count=0;
	recordSize=increrec(count, attributenumber, recordSize, dataTypes, Lengthofthetypedata);

	schema->recordSize=recordSize;
	return schema;
}

RC freeSchema (Schema *schema){
	long int count;
	int a=0;
	for(count=0;count<schema->numAttr;count++){
		free(schema->attrNames[count]);//free all Attribute name
	}
	free(schema->attrNames);//free attrName
	if(schema->attrNames == NULL)
		a++;
	free(schema->dataTypes);//free dataTyoe
	if(schema->dataTypes == NULL)
		a++;
	free(schema->keyAttrs);//free key attributes
	if(schema->keyAttrs == NULL)
		a++;
	free(schema->typeLength);
	if(schema->typeLength == NULL)
		a++;
	free(schema);

	RC_message="Suscessfully freed Schema";
	return RC_OK;
}

RC incre1 (int offset, int i, int NumberofAttribute, Schema *schema) {	
	for(i=0;i<NumberofAttribute;i++){
		int a = sizeof(int);
		int b = sizeof(float);
		int c = sizeof(bool);
		int d = 0;
		switch(schema->dataTypes[i]){
		case DT_BOOL:
			offset+=c;
			break;		
		case DT_INT:
			offset+=a;
			d=d+1;
			break;


		case DT_STRING:
			offset+=schema->typeLength[i];
			d++;
			break;
		case DT_FLOAT:
			offset+=b;
			d=d-1;
			break;
		}
	}
	 return offset;
	
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){
	int sizer = sizeof(Record);
	(*record)=malloc(sizer);
	if(sizer == -1)
		printf("empty record");
	(*record)->data=malloc(schema->recordSize);
	int minu = 0;
	(*record)->id.slot=minu;//Set slot
	(*record)->id.page=minu+1;//Set page
	if(minu<0)
		minu = 0;	

	RC_message="Sucessfully create Record";
	return RC_OK;
}

RC freeRecord (Record *record){
	//Set records free
	free(record->data);
	if(record->data == NULL)
		printf("fred");
	free(record);

	RC_message="Sucessfully freed Record";
	return RC_OK;
}



RC incre2 (int offset, int i, int NumberofAttribute, Schema *schema) {	
	offset = 0;
	for(i=0;i<NumberofAttribute;i++){
		int a = sizeof(int);
		int b = sizeof(float);
		int c = sizeof(bool);
		int d = 0;
		switch(schema->dataTypes[i]){
		case DT_INT:
			offset+=a;
			break;
		case DT_STRING:
			offset+=schema->typeLength[i];
			d++;
			break;
		case DT_FLOAT:
			offset+=b;
			break;
		case DT_BOOL:
			offset+=c;
			break;

		}
	}
	 return NumberofAttribute;
	
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **lengthv){
	int offset=0;
	int count=0;;
	offset = incre1(offset, count, attrNum, schema);
	attrNum = incre2(offset, count, attrNum, schema);

	char *dptr=(record->data)+offset;//pointer to the attribute in record
	
	int sv = sizeof(Value);

	*lengthv = (Value *) malloc(sv);//allocate memory
	(*lengthv)->dt = schema->dataTypes[attrNum];//set data type
	int a = 0;
	switch(schema->dataTypes[attrNum]){
	case DT_BOOL:
		(*lengthv)->v.boolV=*(bool *)dptr;
		a--;
		if(a>100)
			a++;
		break;
	case DT_INT:
		(*lengthv)->v.intV=*(int *)dptr;
		a=a+1;
		if(a==2)	
			a--;
		break;

	
	case DT_STRING:
		//In C language, a string is an array ended by a 0 byte. But in a record, there is no 0 byte in the end of strings
		(*lengthv)->v.stringV = (char *) malloc(schema->typeLength[attrNum] + 1);
		a=a+2;
		memcpy((*lengthv)->v.stringV, dptr, schema->typeLength[attrNum]);
		if(a==4)	a--;
		(*lengthv)->v.stringV[schema->typeLength[attrNum]]='\0';
		a--;
		break;
	case DT_FLOAT://op for different datatype
		(*lengthv)->v.floatV=*(float *)dptr;
		break;
	}

	RC_message="Successfully fetch attribute";
	return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *lengthv){
	//get the attribute's position
		if(schema == NULL){
			return -1;}
	int offset=0;
	int count=0;
	offset = incre1(offset, count, attrNum, schema);
	attrNum = incre2(offset, count, attrNum, schema);

	char *pointer=(record->data)+offset;//set pointer to attribute

	switch(schema->dataTypes[attrNum]){
	case DT_BOOL:
		*(bool *)pointer=lengthv->v.boolV;
		RC_message="BOOL case";
		break;
	case DT_INT:
		*(int *)pointer=lengthv->v.intV;
		RC_message="INT case";
		break;
	case DT_FLOAT:
		*(float *)pointer=lengthv->v.floatV;
		RC_message="FLOAT case";
		break;
	
	case DT_STRING:
		memcpy(pointer, lengthv->v.stringV, schema->typeLength[attrNum]);
		break;
	}

	RC_message="Successfully set Attribute";
	return RC_OK;
}
