#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"
#include "expr.h"
#include "buffer_mgr.h"

// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
} BTreeHandle;

typedef struct MetaData{
	int n;//n for the B+tree
	int keyType;//keyType for the B+tree
	int rootPage;//page number of root
	int numEntries;//total number of entries
	int numEmpty;//number of empty pages, happen when deletion occur
	int emptyList[PAGE_SIZE/sizeof(int)-4];//array of empty pages
} MetaData;

typedef struct BTreeMgmt{
	BM_BufferPool *bm;//pointer to buffer pool
	MetaData *meta;//pointer to meta data
} BTreeMgmt;

typedef struct InNode{
	bool isLeaf;
	int num_keys;
	int pointer[201];
	char key[200][12];
}InNode;

typedef struct LeafNode{
	bool isLeaf;
	int num_keys;
	int next;//pointer to next leaf
	RID pointer[200];
	char key[200][12];
}LeafNode;

typedef struct KeyPair{
	char key[12];//key
	int rpointer;//page pointer
}KeyPair;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;

typedef struct BTScanMgmt{
	BM_PageHandle *page;
	int slot;
}BTScanMgmt;

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC keyCmp (Value *left, char *right, int *result);
extern bool findSlot(char keyarr[][12], int num_keys, Value *key, int *slot);
extern void findPtrSlot(char keyarr[][12], int num_keys, Value *key, int *slot);
extern void writeKey(char *cptr,Value *key);
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern KeyPair insertUnderNode(BTreeHandle *tree, int pageNum, Value *key, RID rid, RC *rcptr);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern bool deleteUnderNode(BTreeHandle *tree, int pageNum, Value *key, RC *rcptr);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);
void preorder(BTreeHandle *tree,int pageNum);
void printKey(DataType dt,char *key);

//shift i to i+i, i+1 to i+2, and so forth, until arr[num_keys-1] to arr[num_keys]
#define SHIFT_KEY_FORTH(arr,i,num_keys) \
	memcpy(arr[i+1],arr[i],sizeof(arr[0])*(num_keys-i));
#define SHIFT_PTR_FORTH(ptr,i,num_keys) \
	memcpy(ptr+i+1,ptr+i,sizeof(ptr[0])*(num_keys-i+1));//pointer has one more
//shift i+1 to i, i+2 to i+1, and so forth, until arr[num_keys-1] to arr[num_keys-2]
#define SHIFT_KEY_BACK(arr,i,num_keys) \
	memcpy(arr[i],arr[i+1],sizeof(arr[0])*(num_keys-i-1));
#define SHIFT_PTR_BACK(ptr,i,num_keys) \
	memcpy(ptr+i,ptr+i+1,sizeof(ptr[0])*(num_keys-i));//pointer has one more

#endif // BTREE_MGR_H
