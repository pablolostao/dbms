#include "btree_mgr.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>


int BufferSize;

// Initialize IndexManager

RC initIndexManager (void *mgmtData){

	if (mgmtData==NULL){
		BufferSize=1000;//Set buffer to 4MB while mgmeData is null
	}
	else if(mgmtData != NULL){
		BufferSize=*(int *)mgmtData;//Set buffer while mgmeData not null
	}
	return RC_OK;

}
RC shutdownIndexManager (){
	printf("Index manager shutted down");
	return RC_OK;
}

//btree index operations
RC createBtree (char *idxId, DataType keyType, int n){
	RC rc;
	int SFsize = sizeof(SM_FileHandle);
	int count = 1;
	while((rc=createPageFile(idxId))!=RC_OK){//create the page file
		printf("Failed to create page file\n");
		return RC_FILE_NOT_FOUND;
	}

	SM_FileHandle *fHandle=malloc(SFsize);
	while((rc=openPageFile(idxId,fHandle))!=RC_OK){//open the page file
		printf("Failed to open page file\n");
		return RC_FILE_NOT_OPENED;
	}
	char buffer_sizse[PAGE_SIZE];
	MetaData *meta=(MetaData *)buffer_sizse;
	switch(count) {
		case 1:meta->numEntries=0;		
		case 2:meta->keyType=keyType;	
		case 3:meta->numEmpty=0;
		case 4:meta->rootPage=1;
		case 5:meta->n=n;
	}
	while((rc=writeBlock(0,fHandle,(SM_PageHandle)buffer_sizse))!=RC_OK){//write to disk
		printf(" ");
		return rc;
	}
	bool flag = true;
	appendEmptyBlock(fHandle);//append a empey page as a empty node
	int no_key	 = 0;
	LeafNode *node=(LeafNode *)buffer_sizse;
	node->next=no_key-1;
	if(flag == true) {
		node->isLeaf=flag;
		node->num_keys=no_key;
	}
	while((rc=writeBlock(1,fHandle,(SM_PageHandle)buffer_sizse))!=RC_OK)
		return rc;

	if((rc=closePageFile(fHandle))!=RC_OK){//close the file
		printf("File closed");
		return rc;
	}
	free(fHandle);//free the malloc
	printf("Successfully create B_tree");
	return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId){
	BM_BufferPool *bm=MAKE_POOL();
	RC rc;
	bool flag = true;
	if((rc=initBufferPool(bm, (const char *) idxId, BufferSize, RS_CLOCK, NULL))!=RC_OK){
	//tell whether the bufferpool is successfully initiated
	//if not, return bufferpool not initiate
		printf("Initiate bufferpool failed");
		return RC_BUFFERPOOL_NOT_INIT;
	}
	BM_PageHandle *page=MAKE_PAGE_HANDLE();
	pinPage(bm,page,0);
	if(flag == true) {
		switch(flag) {
			case true:(*tree)=malloc(sizeof(BTreeHandle));
			(*tree)->idxId=idxId;
			case false:(*tree)->mgmtData=malloc(sizeof(BTreeMgmt));
			BTreeMgmt *management_data=(*tree)->mgmtData;
			if(flag==true) {
				management_data->bm=bm;
				management_data->meta=(MetaData *)(page->data);
				(*tree)->keyType=management_data->meta->keyType;
			}
			}
	}
	markDirty(bm,page);	
	printf("Successfully opened B_Tree.");
	free(page);
	return RC_OK;
}

RC closeBtree (BTreeHandle *tree){
	int a =1, b=1;
	BTreeMgmt *managment_data=tree->mgmtData;
	switch(a){
		case 1:shutdownBufferPool(managment_data->bm);//close buffer
		case 2:free(managment_data->bm);//clean buffer memory
		case 3:if(b == 1) free(managment_data);//set mgmt free
		//clean management data
		case 4:free(tree);//clean the tree
	}
	printf("Successfully closed B_Tree.");
	return RC_OK;
}

RC deleteBtree (char *idxId){
	printf("Page file deleted");
	return destroyPageFile(idxId);
}

// read value of B_Tree nodes

RC getNumNodes (BTreeHandle *tree, int *result){
	BTreeMgmt *managment_data=tree->mgmtData;
	//result equals to total number of pages minus one since each node takes a page 
	if(tree == NULL) {
		return RC_IM_KEY_NOT_FOUND;
	}
	*result=((BM_PFHandle *)managment_data->bm->mgmtData)->fh->totalNumPages-1-managment_data->meta->numEmpty;
	return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result){
	*result=tree->keyType;
	if(tree == NULL) {
	//return key not found if tree is empty
		return RC_IM_KEY_NOT_FOUND;
	}
	return RC_OK;
}
// get eumber entries of B_tree
RC getNumEntries (BTreeHandle *tree, int *result){
	if(tree == NULL) {
		return RC_IM_KEY_NOT_FOUND;
	}
	*result=((BTreeMgmt *)tree->mgmtData)->meta->numEntries;
	printf(" ");
	return RC_OK;
}



//different type of key type
RC keyCmp (Value *left, char *right, int *result){
	//switch by key type
	switch(left->dt) {
	case DT_STRING:
		RC_message="string case";
		*result = strcmp(left->v.stringV, right);		
		break;
	case DT_FLOAT:
		RC_message="float case";
		*result = (left->v.floatV > *(float *)right) - (left->v.floatV < *(float *)right);		
		break;
	case DT_BOOL:
		*result = (left->v.boolV > *(bool *)right) - (left->v.boolV < *(bool *)right);
		RC_message="bool case";
		break;
	case DT_INT:
		*result = (left->v.intV > *(int *)right) - (left->v.intV < *(int *)right);
		RC_message="int case";
		break;
	}

	return RC_OK;
}


bool findSlot(char keyarr[][12], int num_keys, Value *key, int *slot){
//use binary search to find this key's slot and get slot number 
	int m;
	int min=0, max=num_keys-1;
	int cmp;
	int a=0;
	while(1){
		if (max<min){
		//error condition, return false
			int w=0;
			w=w+1;
			*slot=min;
			return false;
		}
		int sum = min+max;
		int half = sum/2;
		m=half;;
		keyCmp(key, keyarr[m], &cmp);
		if(cmp<0) a=1;
		else if(cmp>0) a=2;
		else a=3;
		switch(a){
			case 1: max=m-1; break;
			case 2: min=m+1; break;
			case 3: *slot=m; return true;
		
		
		}
//if find, return true; not find, return false;

	}

}

void findPtrSlot(char keyarr[][12], int num_keys, Value *key, int *slot){
//move pointer position is the slot is the
	int count = 0;
	if (findSlot(keyarr,num_keys,key,slot)){
		(*slot)=(*slot)+1;
		count=count+1;
	}
}


void writeKey(char *cptr,Value *key){
	switch(key->dt){
	case DT_STRING:
		strcpy(cptr,key->v.stringV);
		RC_message="string case";
		break;

	case DT_FLOAT:
		*(float *)cptr=key->v.floatV;
		RC_message="float case";
		break;
	case DT_BOOL:
		*(bool *)cptr=key->v.boolV;
		RC_message="bool case";
		break;
	case DT_INT:
		*(int *)cptr=key->v.intV;
		RC_message="int case";
		break;

	}
}

RC findKey (BTreeHandle *tree, Value *key, RID *result){
	if(tree == NULL) {
		return RC_IM_KEY_NOT_FOUND;
	}
	if (key->dt!=tree->keyType){
		printf("Key type is incorrect");
		return RC_IM_KEY_ERROR;
	}
	bool flag=false;
	BTreeMgmt *managment_data=tree->mgmtData;
	int a=1;
	BM_BufferPool *bm=managment_data->bm;
	int car = managment_data->meta->rootPage;
	BM_PageHandle *page=MAKE_PAGE_HANDLE();

	int nextPage=car;
	char *data_pointer;
	bool atLeaf=flag;
	RC rc;
	while(atLeaf==false){
		pinPage(bm,page,nextPage);
		data_pointer=page->data;
		if(*(bool *)data_pointer==true){
			flag=true;
			atLeaf=flag;
			int slot_number;
			LeafNode *node=(LeafNode *)data_pointer;
			
			if(a==1){
				if (findSlot(node->key, node->num_keys, key, &slot_number)){
					printf("Key is found");
					rc=RC_OK;
					*result=node->pointer[slot_number];
				}else{
					printf("Did not find the key, key don't exsit");
					rc=RC_IM_KEY_NOT_FOUND;
				}
			}
		}else{
			InNode *node=(InNode *)data_pointer;
			int nnk=node->num_keys;			
			int slot_number;
			findPtrSlot(node->key, nnk, key, &slot_number);
			if(a == 111){
				return rc;
			}
			nextPage=node->pointer[slot_number];
		}
		unpinPage(bm,page);
	}

	free(page);
	return rc;
}



KeyPair insertUnderNode(BTreeHandle *tree, int pageNum, Value *key, RID rid, RC *rcptr){
	bool emp=false;	
	BTreeMgmt *management_data=tree->mgmtData;
	if(tree==NULL){
		emp=true;
	}
	BM_BufferPool *bm=management_data->bm;
	BM_PageHandle *location=MAKE_PAGE_HANDLE();
	BM_PageHandle *page=location;
	int minu=-1;
	pinPage(bm,page,pageNum);
	KeyPair ret;

	char *pointer_data=page->data;
	char *pdata=pointer_data;
	if (*(bool *)pdata==false){
		InNode *node=(InNode *)pdata;
		int qurt=node->num_keys;
		int slot;
		bool rank=false;
		findPtrSlot(node->key, qurt, key, &slot);
		int nextPage=node->pointer[slot];		
		KeyPair keyPair=insertUnderNode(tree, nextPage, key, rid, rcptr);
		if(node->num_keys<management_data->meta->n){
			rank=true; 
		}
		if (keyPair.rpointer==-1){
		
			ret.rpointer=minu;
		}else if (rank==true){
			switch(minu){
				case -1:{SHIFT_KEY_FORTH(node->key,slot,node->num_keys);}
				case 3:{SHIFT_PTR_FORTH(node->pointer,(slot+1),node->num_keys);}
				case 4:{memcpy(node->key[slot],keyPair.key,12);}
				case 5:{node->pointer[slot+1]=keyPair.rpointer;}
				}
				node->num_keys=node->num_keys+1;
				ret.rpointer=-1;
				markDirty(bm,page);
		}else{
			BM_PageHandle *rpage=MAKE_PAGE_HANDLE();
			bool is_l=true;
			long int page_num_new;
			int t=0;
			if (management_data->meta->numEmpty>0){
				t=1;
			}else {
				t=2;
			}
			switch(t) {
				case 1: page_num_new=management_data->meta->emptyList[--(management_data->meta->numEmpty)];
					break;
				case 2: page_num_new=((BM_PFHandle *)bm->mgmtData)->fh->totalNumPages;
			}
			pinPage(bm,rpage,page_num_new);
			if(is_l==true){
				is_l=false;
			}
			InNode *right_node=(InNode *)rpage->data;
			right_node->isLeaf=is_l;
			int nk=node->num_keys;
			int left_number=(nk+1)/2;
			int right_number=nk-left_number;
			ret.rpointer=rpage->pageNum;
			if (slot<=left_number){
				memcpy(right_node->key[0],node->key[left_number],12*(nk-left_number));
				int quin=0;
				memcpy(right_node->pointer,node->pointer+left_number,sizeof(int)*(right_number+1));
				
				if (slot!=left_number){
					quin=1;
				} else{
					quin=2;
				}
				switch(quin) {
					case 1:{
						int new_left = left_number-1;
						memcpy(ret.key,node->key[new_left],12);
						int nf=new_left;
						SHIFT_KEY_FORTH(node->key,slot,nf);
						SHIFT_PTR_FORTH(node->pointer,(slot+1),new_left);
						int dosen=12;
						memcpy(node->key[slot],keyPair.key,dosen);
						node->pointer[slot+1]=keyPair.rpointer;
						break;
					}
					case 2:{
						memcpy(ret.key,keyPair.key,12);
						int lfnb=left_number;
						node->pointer[lfnb]=keyPair.rpointer;
					}
				}
			}else{
				
				memcpy(ret.key,node->key[left_number],12);
				int sln=sizeof(int)*(slot-left_number);
				int nnks=node->num_keys-slot;
				int lft1=left_number+1;
				memcpy(right_node->key[0],node->key[lft1],12*(slot-left_number-1));
				//at right position, add a copy of key
				memcpy(right_node->pointer,node->pointer+lft1,sln);
				//at right position, add a copy of pointer
				int snn=sizeof(int)*(node->num_keys-slot);
				memcpy(right_node->key[slot-left_number-1],keyPair.key,12);
				int count=1;;
				//Make a insertion of key here
				right_node->pointer[slot-left_number]=keyPair.rpointer;
				if(lft1 != 0){
					
					count=count+1;
				}
				//Make a insertion of pointer here
				memcpy(right_node->key[slot-left_number],node->key[slot],12*nnks);
				//at right position, add a copy of key
				memcpy(right_node->pointer+slot-left_number+1,node->pointer+slot+1,snn);
			}
			node->num_keys=left_number;
			right_node->num_keys=right_number;
			//finished add at non-leaf circumstance
			markDirty(bm,page);
			markDirty(bm,rpage);
			unpinPage(bm,rpage);
			//free right page after insertion
			free(rpage);
		}
	}else{
		
		int mius=-1;
		int slot;
		LeafNode *node=(LeafNode *)pdata;
		if (findSlot(node->key, node->num_keys, key, &slot)){//check if key existed already
			printf("This key has alredy created");
			*rcptr=RC_IM_KEY_ALREADY_EXISTS;
			ret.rpointer=mius;
		}else if (node->num_keys<management_data->meta->n){//no need to split
			switch(mius){
				case -1:{SHIFT_KEY_FORTH(node->key,slot,node->num_keys);}
				case 1:{SHIFT_PTR_FORTH(node->pointer,slot,node->num_keys);}
				case 2:{writeKey(node->key[slot],key);}
			}
			node->pointer[slot]=rid;
			node->num_keys++;
			ret.rpointer=mius;
			markDirty(bm,page);
		}else{
			//overflow, the leafnode needs to be splitted into two
			int y=0;
			BM_PageHandle *right_page=MAKE_PAGE_HANDLE();
			int page_num_new;
			
			if (management_data->meta->numEmpty>0){
				y=1;
			}else{
				y=2;
			}
			long int parthen=0;
			switch(y){
				case 1:{page_num_new=management_data->meta->emptyList[--(management_data->meta->numEmpty)];}
				case 2:{page_num_new=((BM_PFHandle *)bm->mgmtData)->fh->totalNumPages;}
			}
			pinPage(bm,right_page,page_num_new);
			bool isl=true;
			LeafNode *right_node=(LeafNode *)right_page->data;
			right_node->isLeaf=isl;
			right_node->next=node->next;
			int nnk=node->num_keys;
			node->next=page_num_new;
			int right_number=(nnk+1)/2;
			int left_number=(nnk+1)-right_number;
			ret.rpointer=right_page->pageNum;
			if (slot<((nnk+1)-right_number)){
				memcpy(right_node->key[0],node->key[left_number-1],12*((nnk+1)/2));
				//at right position, add a copy of key
				int count=1;
				memcpy(right_node->pointer,node->pointer+left_number-1,sizeof(node->pointer[0])*right_number);
				//at right position, add a copy of pointer
				int dosen=12;
				memcpy(ret.key,right_node->key[0],dosen);
				if(nnk == 0){count = count +1;}
				SHIFT_KEY_FORTH(node->key,slot,left_number-1);
				//move key to forward
				SHIFT_PTR_FORTH(node->pointer,slot,left_number-1);
				count=2;
				//move pointer forward
				writeKey(node->key[slot],key);
				node->pointer[slot]=rid;
			}else{
				int sln=slot-left_number;
				memcpy(right_node->key[0],node->key[left_number],12*sln);
				//at right position, add a copy of key
				int snp=sizeof(node->pointer[0]);
				memcpy(right_node->pointer,node->pointer+left_number,snp*sln);
				//at right position, add a copy of pointer
				writeKey(right_node->key[slot-left_number],key);
				right_node->pointer[slot-left_number]=rid;
				int nnks=node->num_keys-slot;
				memcpy(right_node->key[slot-left_number+1],node->key[slot],12*nnks);
				//at right position, add a copy of key
				memcpy(right_node->pointer+slot-left_number+1,node->pointer+slot,snp*nnks);
				//at right position, add a copy of pointer

				memcpy(ret.key,right_node->key[0],12);
			}
			node->num_keys=left_number;
			right_node->num_keys=right_number;
			//finished add at leafnode circumstance
			markDirty(bm,page);
			parthen=parthen+1;
			if(parthen==3){parthen=parthen-1;}
			markDirty(bm,right_page);
			unpinPage(bm,right_page);
			//free current page
			free(right_page);
		}
		printf("Successfully inserted key!");
		*rcptr=RC_OK;
	}
	unpinPage(bm,page);
	if(emp!=false){minu=-1;}
	free(page);
	return ret;
}


RC insertKey (BTreeHandle *tree, Value *key, RID rid){
	BTreeMgmt *management_data=tree->mgmtData;
	RC rc;
	if (key->dt!=tree->keyType){
		if(tree == NULL) {
			return RC_IM_KEY_NOT_FOUND;
		}
		printf("Key type incorrect");
		return RC_IM_KEY_ERROR;
	}
	
	int page_number=management_data->meta->rootPage;
	int minu=-1;
	KeyPair keyPair=insertUnderNode(tree, page_number, key, rid, &rc);
	//create a new root for this insertion
	if (keyPair.rpointer!=minu){
	
		int new_Page_Number;
		BM_BufferPool *bm=management_data->bm;
		int q=1;
		BM_PageHandle *page=MAKE_PAGE_HANDLE();		
		
		if (management_data->meta->numEmpty>0)	{q=1;}
		else	{q=2;}
		switch(q){
		case 1:{
			new_Page_Number=management_data->meta->emptyList[--(management_data->meta->numEmpty)];
		}
		case 2:{
			new_Page_Number=((BM_PFHandle *)bm->mgmtData)->fh->totalNumPages;
		}
		}
		pinPage(bm,page,new_Page_Number);
		bool flag=false;
		InNode *root=(InNode *)page->data;
		switch(flag){
			case false:root->isLeaf=flag;
			case true:root->num_keys=1;
		}
		memcpy(root->key[0],keyPair.key,12);
		switch(flag){
			case false:root->pointer[0]=page_number;//set pinter at position 0 to be page number
			case true:root->pointer[1]=keyPair.rpointer;
		}
		management_data->meta->rootPage=new_Page_Number;
		if(flag==false){
			flag=true;	
		}
		markDirty(bm,page);
		//mark page dirty
		unpinPage(bm,page);
		
		//unpin cuurent pge
		free(page);
		//free page
	}
	//set management data to updated
	management_data->meta->numEntries=management_data->meta->numEntries+1;
	return rc;
}


RC deleteKey (BTreeHandle *tree, Value *key){
	if (key->dt!=tree->keyType){
		printf("Key type is incorrect");
		return RC_IM_KEY_ERROR;
	}
	bool flag = false;
	BTreeMgmt *management_data=tree->mgmtData;
	RC rc;
	int pageNum=management_data->meta->rootPage;
	if(flag==false) deleteUnderNode(tree,pageNum,key,&rc);
	flag=true;
	management_data->meta->numEntries--;
	RC_message="key deleted";
	return rc;
}


/************************************************************************************************************************************************************/
//delete the key which is either in the node pageNum or descendant of pageNum
//pass the result argument rc, return whether it results in underflow of current node or not
bool deleteUnderNode(BTreeHandle *tree, int pageNum, Value *key, RC *rcptr){
	
	if(tree == NULL) {
		return RC_IM_KEY_NOT_FOUND;
	}
	bool res;
	BTreeMgmt *managment_data=tree->mgmtData;
	BM_BufferPool *bm=managment_data->bm;
	BM_PageHandle *page=MAKE_PAGE_HANDLE();
	//return value

	pinPage(bm,page,pageNum);
	char *pdata=page->data;
	if (*(bool *)pdata==false){//none leaf
		int slot;
		InNode *node=(InNode *)pdata;
		int pnum = node->num_keys;
		findPtrSlot(node->key, pnum, key, &slot);
		if (deleteUnderNode(tree, node->pointer[slot], key, rcptr) == false){//no underflow, we are done
			res=false;
		}else{//has underflow
			BM_PageHandle *cpage=MAKE_PAGE_HANDLE();//page handle
			pinPage(bm,cpage,node->pointer[slot]);//pin child page
			pdata=cpage->data;
			BM_PageHandle *leftpage=MAKE_PAGE_HANDLE();//left sibling page
			BM_PageHandle *rightpage=MAKE_PAGE_HANDLE();//right sibling page
			if (*(bool *)pdata == true){//child is leaf
				LeafNode *child=(LeafNode *)pdata;
				LeafNode *left=0;
				LeafNode *right=0;
				if (slot>0){//has left sibling
					int leftSlot = slot-1;
					pinPage(bm,leftpage,node->pointer[leftSlot]);//pin left sibling
					left=(LeafNode *)leftpage->data;
				}
				if (slot<node->num_keys){
					int rightSlot = slot+1;//has right sibling
					pinPage(bm,rightpage,node->pointer[rightSlot]);//pin right sibling page
					right=(LeafNode *)rightpage->data;
				}

				int rekey = (managment_data->meta->n+1)/2;
				bool remark = true;
				if (left && left->num_keys>rekey){//left has key to redistribute 
					int childnum =  child->num_keys;
					int leftnum = left->num_keys;
					SHIFT_KEY_FORTH(child->key,0,childnum);//move key forward
					SHIFT_PTR_FORTH(child->pointer,0,childnum);//move ptr forward

int nums_key = 12;

					memcpy(child->key[0],left->key[leftnum-1],nums_key);//move one key
					child->pointer[0]=left->pointer[leftnum-1];//move the pointer

					child->num_keys = child->num_keys+1;
					left->num_keys = left->num_keys-1;
				
					memcpy(node->key[slot-1],child->key[0],12);//update the key in parent
				if(remark){
					markDirty(bm,leftpage);//mark left as dirty
					markDirty(bm,cpage);//mark child page as dirty
					}
					
				}else if (right && right->num_keys>rekey){//right has key to redistribute
					int childnum =  child->num_keys;
					int rightnum = right->num_keys;
					memcpy(child->key[childnum],right->key[0],12);//copy the first of right
					child->pointer[childnum]=right->pointer[0];//copy the pointer
					child->num_keys= child->num_keys + 1;//update num_keys

					SHIFT_KEY_BACK(right->key,0,rightnum);//shift keys backward in right
					SHIFT_PTR_BACK(right->pointer,0,rightnum);//shift ptrs back
					right->num_keys = right->num_keys - 1;//update num_keys

					memcpy(node->key[slot],right->key[0],12);//update the key in parent
				if(remark){
					markDirty(bm,rightpage);//mark right page as dirty
					markDirty(bm,cpage);//mark child page as dirty
					}
				}else if (left){//merge with left sibling
					int childnum =  child->num_keys;
					int leftnum = left->num_keys;
					int keys = node->num_keys;
					memcpy(left->key[leftnum],child->key[0],12*childnum);
					//copy keys from child to left
					memcpy(left->pointer+leftnum,child->pointer,\
							sizeof(child->pointer[0])*childnum);//copy ptr from child to left
					left->num_keys = left->num_keys + child->num_keys;//update num_keys of left node
					left->next=child->next;//set next of child to be next of left
					int listnum = managment_data->meta->numEmpty++;
					managment_data->meta->emptyList[listnum]=cpage->pageNum;
					//add the removed page to empty list
					int lslot = slot - 1;
					SHIFT_KEY_BACK(node->key,lslot,keys);//move keys back in parent
					SHIFT_PTR_BACK(node->pointer,slot,keys);//move pointers back in parent
					keys = keys - 1;//update the number of keys in parent
				if(remark){
					markDirty(bm,page);//mark parent as dirty
					markDirty(bm,leftpage);//mark left as dirty
				}
				}else{//merge with right sibling
					int childnum =  child->num_keys;
					int rightnum = right->num_keys;
					int keys = node->num_keys;
					memcpy(child->key[childnum ],right->key[0],12*rightnum);
					//copy keys from right to child
					memcpy(child->pointer+childnum ,right->pointer,\
							sizeof(right->pointer[0])*rightnum);//copy ptrs from right to child
					child->num_keys= child->num_keys + right->num_keys;
					child->next=right->next;//set next of child as next of right
					int numempty = managment_data->meta->numEmpty++;
					managment_data->meta->emptyList[numempty]=rightpage->pageNum;
					//add right to empty list

					SHIFT_KEY_BACK(node->key,slot,keys);//move keys back in parent
					int rslot = slot+1;
					SHIFT_PTR_BACK(node->pointer,rslot,keys);//move ptrs back in parent
					keys=keys-1;//update the number of keys in parent
				if(remark){
					markDirty(bm,page);//mark parent as dirty
					markDirty(bm,cpage);//mark child as dirty
				}
				}
				int i = 0;
				if(left){i = 1;}
				else if(right){i = 2;}

				switch(i){
					case 1:{
					unpinPage(bm,leftpage);
					break;
				}
					case 2:{
					unpinPage(bm,rightpage);
				}
			}
				
			}else{//child is non leaf
				InNode *child=(InNode *)pdata;
				InNode *left=0;
				InNode *right=0;
				int keys = node->num_keys;
				if (slot>0){//has left sibling
					int inslot = slot-1;
					pinPage(bm,leftpage,node->pointer[inslot]);//pin left sibling
					left=(InNode *)leftpage->data;
				}
				if (slot<keys){//has right sibling
					int rslot = slot+1;
					pinPage(bm,rightpage,node->pointer[rslot]);//pin right sibling page
					right=(InNode *)rightpage->data;
				}
				int range = managment_data->meta->n/2;
				if (left && left->num_keys>range){//left has key to distribute
					int childnum =  child->num_keys;
					int leftnum = left->num_keys;
					SHIFT_KEY_FORTH(child->key,0,childnum);//move keys forward
					SHIFT_PTR_FORTH(child->pointer,0,childnum);//move ptrs forward
					int lslot = slot-1;
					memcpy(child->key[0],node->key[lslot],12);//copy key from parent to child
					child->pointer[0]=left->pointer[leftnum];//copy pointer from left to child
					memcpy(node->key[lslot],left->key[leftnum-1],12);//copy key from left to parent

					left->num_keys = left->num_keys-1;//update number of keys in left
					child->num_keys = child->num_keys+1;//update number of keys in child
					markDirty(bm,leftpage);//mark left as dirty
					markDirty(bm,page);//mark parent as dirty
					markDirty(bm,cpage);//mark child as dirty
				}else if(right && right->num_keys>range){//right has key to distribute
					int childnum =  child->num_keys;
					int rightnum = right->num_keys;
					memcpy(child->key[childnum],node->key[slot-1],12);//move key from parent to child
					child->pointer[childnum+1]=right->pointer[0];//move pointer from right to left
int rslot = slot-1;
					memcpy(node->key[rslot],right->key[0],12);//move key from right to parent

					SHIFT_KEY_BACK(right->key,0,rightnum);//shift keys back
					SHIFT_PTR_BACK(right->pointer,0,rightnum);//shift ptrs back

					child->num_keys = child->num_keys+1;//update number of keys in child
					right->num_keys = right->num_keys-1;//update number of keys in right
					markDirty(bm,cpage);
					markDirty(bm,page);
					markDirty(bm,rightpage);//mark dirty
				}else if(left){//merge with left
					int childnum =  child->num_keys;
					int leftnum = left->num_keys;
					int keys = node->num_keys;
					int lslot = slot-1;
					bool remark = true;
					memcpy(left->key[leftnum],node->key[lslot],12);
					//move the key from parent down to left

					memcpy(left->key[leftnum+1],child->key[0],12*childnum);
					//copy keys from child to left
					memcpy(left->pointer+leftnum+1,child->pointer,\
							sizeof(child->pointer[0])*(1+childnum));//copy ptrs from child to left
					left->num_keys=left->num_keys+(1+childnum);//update number of keys in left

					SHIFT_KEY_BACK(node->key,(lslot),keys);//move keys back in parent
					SHIFT_PTR_BACK(node->pointer,slot,keys);//move ptrs back in parent
					node->num_keys =  node->num_keys-1;//update number of keys in parent

					managment_data->meta->emptyList[managment_data->meta->numEmpty++]=cpage->pageNum;
					//add child page to empty list

					if(remark){
					markDirty(bm,page);//mark parent as dirty
					markDirty(bm,leftpage);//mark left as dirty
					}

				}else{//merge with right
					int childnum =  child->num_keys;
					int rightnum = right->num_keys;
					int keys = node->num_keys;
					memcpy(child->key[childnum],node->key[slot],12);
					//move the key from parent down to left

					memcpy(child->key[childnum+1],right->key[0],12*rightnum);
					//copy keys from right to child
					memcpy(child->pointer+rightnum+1,right->pointer,\
							sizeof(right->pointer[0])*(1+rightnum));//copy ptrs from right to child
					child->num_keys=child->num_keys + (1+rightnum);//update number of keys in child

					SHIFT_KEY_BACK(node->key,slot,keys);//move keys back in parent
int rslot = slot+1;
					SHIFT_PTR_BACK(node->pointer,rslot,keys);//move pointers back in parent
					node->num_keys = node->num_keys - 1;

					managment_data->meta->emptyList[managment_data->meta->numEmpty++]=rightpage->pageNum;//add right to empty list

					markDirty(bm,page);
					markDirty(bm,cpage);
				}
				int i = 0;
				if(left){i = 1;}
				else if(right){i = 2;}

				switch(i){
					case 1:{
					unpinPage(bm,leftpage);
					break;
				}
					case 2:{
					unpinPage(bm,rightpage);
				}
			}
			free(leftpage);
			free(rightpage);
			unpinPage(bm,cpage);
			free(cpage);
			int under_rekey = (managment_data->meta->n)/2;
			if(node->num_keys<under_rekey){//has underflow
				res=true;//mark there is an underflow
				int keys = node->num_keys;
				if (keys == 0){//parent has no key anymore
				
				managment_data->meta->rootPage=node->pointer[0];//the child becomes new root
				int listsize = managment_data->meta->numEmpty++;
					managment_data->meta->emptyList[listsize]=pageNum;//add parent to empty list
				}
			}else{//no underflow
				res=false;
			}
		}
	}}else{//leaf
		int slot;
		LeafNode *node=(LeafNode *)pdata;
		int keys = node->num_keys;
		if (!findSlot(node->key, keys, key, &slot)){//if not exist
			RC_message="Key not found";
			*rcptr=RC_IM_KEY_NOT_FOUND;
			res=false;
		}else{
			SHIFT_KEY_BACK(node->key,slot,keys);//move keys back
			SHIFT_PTR_BACK(node->pointer,slot,keys);//move pointers back
			node->num_keys = node->num_keys - 1;//reduce num_keys
			markDirty(bm,page);//mark page to be dirty
			int range = (managment_data->meta->n+1)/2;
			int i = 0;			
			if(keys>=range){
				i=1;
			}else{
				i=2;
			}
			switch(i){
			case 1:
			{res = false;
			break;}

			case 2:
			{res = true;
			}
		}

			*rcptr=RC_OK;
		}
	}
	unpinPage(bm,page);
	free(page);
	return res;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
	if(handle == NULL){
		return RC_FILE_NOT_FOUND;
	}
	(*handle)=malloc(sizeof(BT_ScanHandle));
	if(tree == NULL){
		return RC_FILE_NOT_FOUND;
	}
	(*handle)->tree=tree;
	
	BTreeMgmt *managment_data=tree->mgmtData;
	BM_BufferPool *bm=managment_data->bm;
	int nextPage=managment_data->meta->rootPage;
	BM_PageHandle *page=MAKE_PAGE_HANDLE();
	pinPage(bm,page,nextPage);
	while (*(bool *)page->data==false){
		InNode* inode=(InNode *)page->data;
		nextPage=inode->pointer[0];
		unpinPage(bm,page);
		pinPage(bm,page,nextPage);
	}
	BTScanMgmt *scan=(*handle)->mgmtData=malloc(sizeof(BTScanMgmt));
	scan->slot=0;
	scan->page=page;
	return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result){
	if(handle == NULL){
	return RC_FILE_NOT_FOUND;
}
	BTScanMgmt *scan=handle->mgmtData;
	BM_PageHandle *page = scan->page;
	LeafNode *node=(LeafNode *)page->data;
	int slot = scan->slot;
	int key = node->num_keys;
	if(slot == key){//check if overflow
	
		BTreeMgmt *managment_data=handle->tree->mgmtData;
		unpinPage(managment_data->bm,page);
		if(node->next==-1){//no next, reached rightmost leaft
			RC_message="no next entry";
			return RC_IM_NO_MORE_ENTRIES;
		}else{//pin next node
			pinPage(managment_data->bm,page,node->next);
			node=(LeafNode *)page->data;
			scan->slot=0;
		}
	}
	int next_slot = scan->slot++;
	*result=node->pointer[ next_slot];
	RC_message="get nextEntry successful";
	return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle){
	if(handle == NULL){
	return RC_FILE_NOT_FOUND;
}
	BTScanMgmt *scan=handle->mgmtData;
	BM_PageHandle *page = scan->page;
	free(page);
	free(scan);
	free(handle);
	RC_message=" closed TreeScan";
	return RC_OK;
}

// debug and test functions
char *printTree (BTreeHandle *tree){
	BTreeMgmt *managment_data=tree->mgmtData;
	preorder(tree,managment_data->meta->rootPage);
	return NULL;
}

void preorder(BTreeHandle *tree,int pageNum){
	if(tree == NULL) {
		return;
	}
	BTreeMgmt *managment_data=tree->mgmtData;
	BM_BufferPool *bmdata=managment_data->bm;
	BM_PageHandle *page=MAKE_PAGE_HANDLE();
	
	pinPage(bmdata,page,pageNum);

	printf("(%d)[",pageNum);
	if(*(bool *)page->data == true){//is leaf node
		LeafNode *node=(LeafNode *)page->data;
		int keys = node->num_keys;
		for(int i=0;i < keys;i++){
			int page = node->pointer[i].page;
			int slot = node->pointer[i].slot;
			printf("%d.%d,",page,slot);
			DataType type = tree->keyType;
		        char *key = node->key[i];
			printKey(type,key);
		}
		printf("%d]\n",node->next);
	}else{//non-leaf
		InNode *node=(InNode *)page->data;
		int keys = node->num_keys;
		for(int i=0;i<keys;i++){
			DataType type = tree->keyType;
		        char *key = node->key[i];
			printf("%d,",node->pointer[i]);
			printKey(type,key);
		}
		printf("%d]\n",node->pointer[node->num_keys]);
		for(int i=0;i < keys+1 ;i++){
			int num = node->pointer[i];
			preorder(tree,num);
		}
	}
}

void printKey(DataType dt,char *key){
	if(key != NULL){
		switch(dt){
	case DT_INT:
		printf("%d,",*(int *)key);
		RC_message="DataType：DT_INT";
		break;
	case DT_FLOAT:
		printf("%f,",*(float *)key);
		RC_message="DataType：DT_FLOAT";
		break;
	case DT_BOOL:
		printf("%d,",*(bool *)key);
		RC_message="DataType：DT_BOOL";
		break;
	case DT_STRING:
		RC_message="DataType：DT_STRING";
		printf("%s,",key);
	}
	}
	
}
