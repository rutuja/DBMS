
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <algorithm>
#include <queue>

#include "../pf/pf.h"
#include "../rm/rm.h"

#define IX_EOF (-1)  // end of the index scan
#define INDEX_TABLE_NAME "Indexes"
#define FANOUT 128
#define NULLENTRY -1

using namespace std;

typedef enum { IndexPage = 0, LeafPage, OverflowPage } IX_PageType;

typedef struct{
	int key;
	unsigned pageid;
}IndexPage_TypeIntAttr;

typedef struct{
	float key;
	unsigned pageid;
}IndexPage_TypeRealAttr;

typedef struct{
	char key[4];
	unsigned pageid;
}IndexPage_TypeVarcharAttr;

typedef struct{
	int key;
	int hasDuplicate;
	RID rid;
}LeafPage_TypeIntAttr;

typedef struct{
	float key;
	int hasDuplicate;
	RID rid;
}LeafPage_TypeRealAttr;

typedef struct{
	char key[4];
	int hasDuplicate;
	RID rid;
}LeafPage_TypeVarcharAttr;

class IX_IndexHandle;

class IX_Manager {
 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index
  
  //INTERNAL METHODS

  void CreateIndexTable();

  RC CreateIndexName(const string tableName, const string attributeName, void* indexName, unsigned int* indexName_size);

  RC prepareIndexTuple( const char *indexName , const unsigned int indexNameLength
		  	  	  	  , const char *tableName , const unsigned int tableNameLength
		  	  	  	  , const char *attributeName , const unsigned int attributeNameLength
		  	  	  	  , const char *indexfileName , const unsigned int indexfileNameLength
		  	  	  	  , void *tuple, unsigned int *tuple_size);
  RC IX_ScanTableInsertIndex(const string tableName, const string attributeName,IX_IndexHandle &IX_handle);
  RC IX_PrepareEmptyRootOrIndexTuple(void *tuple);

 protected:
  IX_Manager   ();                             // Constructor
  ~IX_Manager  ();                             // Destructor
 
 private:
  static IX_Manager *_ix_manager;
};


class IX_IndexHandle {
 public:
  IX_IndexHandle  ();                           // Constructor
  ~IX_IndexHandle ();                           // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry

  //INTERNAL METHODS

  RC Insert(void *key, const RID &rid,PageNum pagenum,void * splittedIndexNode);
  RC printIndex();
  RC IX_CreateNode(IX_PageType pageType,PageNum &pagenum);

   void IX_SortVector(vector<void*>& keyrids,AttrType attrType,IX_PageType ix_pageType);
   RC IX_CreateBufferPage(unsigned pageNum, unsigned leftPointerPageNum,unsigned rightPointerPageNum,void* IX_buffer, IX_PageType ix_pageType, vector<void*>& keyrids);
   RC IX_InitializeBuffer(void* IX_buffer,IX_PageType ix_pageType,unsigned leftPointerPageNum,unsigned rightPointerPageNum);
   void IX_CalculateRecLen(unsigned int* recLen,IX_PageType ix_pageType);
   void IX_insertTupleIntoBuffer(unsigned pageNum, void* IX_buffer, void* IX_tuple, unsigned recLen);
   void IX_CalculateNumRecsOffsetFreeSpaceInPage(void* IX_buffer, unsigned* NumOfRecs, unsigned* RecOffset, unsigned* freeSpace);

   void IX_PreparePageTypeDetailsSlot(void* IX_tuple,IX_PageType IX_pageType,unsigned leftPointerPageNum);
   void IX_PreparePageDetailsSlot(void* IX_tuple, unsigned NumOfRecs, unsigned RecOffset, unsigned freeSpace);

   RC IX_CreateVoidVectorFromPage(unsigned pageNum,unsigned *leftPointerPageNum,unsigned *rightPointerPageNum,IX_PageType *ix_pageType, AttrType attrType, vector<void*>& keyrids);
   RC IX_Create_IndexPage_TypeIntAttr_Vector(vector<void*>& voidkeyrids, vector<IndexPage_TypeIntAttr>& keyrids);
   RC IX_Create_IndexPage_TypeRealAttr_Vector(vector<void*>& voidkeyrids, vector<IndexPage_TypeRealAttr>& keyrids);
   RC IX_Create_IndexPage_TypeVarcharAttr_Vector(vector<void*>& voidkeyrids, vector<IndexPage_TypeVarcharAttr>& keyrids);
   RC IX_Create_LeafPage_TypeIntAttr_Vector(vector<void*>& voidkeyrids, vector<LeafPage_TypeIntAttr>& keyrids);
   RC IX_Create_LeafPage_TypeRealAttr_Vector(vector<void*>& voidkeyrids, vector<LeafPage_TypeRealAttr>& keyrids);
   RC IX_Create_LeafPage_TypeVarcharAttr_Vector(vector<void*>& voidkeyrids, vector<LeafPage_TypeVarcharAttr>& keyrids);
   RC IX_Create_OverflowPage_RIDs_Vector(vector<void*>& voidkeyrids, vector<RID>& keyrids);

   RC IX_Create_IndexPage_TypeIntAttr_VoidVector(vector<void*>& voidkeyrids, vector<IndexPage_TypeIntAttr>& keyrids);
   RC IX_Create_IndexPage_TypeRealAttr_VoidVector(vector<void*>& voidkeyrids, vector<IndexPage_TypeRealAttr>& keyrids);
   RC IX_Create_IndexPage_TypeVarcharAttr_VoidVector(vector<void*>& voidkeyrids, vector<IndexPage_TypeVarcharAttr>& keyrids);
   RC IX_Create_LeafPage_TypeIntAttr_VoidVector(vector<void*>& voidkeyrids, vector<LeafPage_TypeIntAttr>& keyrids);
   RC IX_Create_LeafPage_TypeRealAttr_VoidVector(vector<void*>& voidkeyrids, vector<LeafPage_TypeRealAttr>& keyrids);
   RC IX_Create_LeafPage_TypeVarcharAttr_VoidVector(vector<void*>& voidkeyrids, vector<LeafPage_TypeVarcharAttr>& keyrids);
   RC IX_Create_OverflowPage_RIDs_VoidVector(vector<void*>& voidkeyrids, vector<RID>& keyrids);

   RC IX_UpdatePageWithNewVectorDetails(unsigned pageNum,unsigned leftPointerPageNum,unsigned rightPointerPageNum,
		   	   	   	   	   	   	   	   	   IX_PageType ix_pageType, AttrType attrType, vector<void*>& keyrids);
   RC printIndex_modified();
   //========================================================================================================
     //===============================DELETE ENTRY=============================================================
     //========================================================================================================
   RC IX_DeleteSearch(void *key, const RID &rid,PageNum parentpointer,PageNum *nodepointer,int *oldchildentry,unsigned *searchFound);

   unsigned IX_CompareVoidKeyValues(void* deleteKey, void* pageKey, AttrType attrType);
   unsigned IX_CompareVoidRIDValues(const RID &rid,void* pageKey);
   unsigned IX_ComparePageNumValuesInIndexNode(int oldchildentry, void* pageKey);
   RC IX_GetSiblingDetails(void *key, const RID &rid
		   	   	   	   ,PageNum parentpointer,PageNum nodepointer
		   	   	   	   ,PageNum *leftSiblingPointer,int *numOfKeysInLeftSibling
		   	   	   	   ,PageNum *rightSiblingPointer,int *numOfKeysInRightSibling);
   RC IX_GetnumOfKeysInNode(PageNum nodepointer,int *numOfKeysInNode);
   RC IX_Redistribute(PageNum parentPagePointer,PageNum leftPagePointer,PageNum rightPagePointer);
   RC IX_Merge(PageNum parentPagePointer,PageNum leftPagePointer,PageNum rightPagePointer,int *oldchildentry);
   RC IX_CompareVoidRIDValuesFromOverFlow(const RID &rid,PageNum OverflowPageNum);
     //========================================================================================================
     //===============================DELETE ENTRY=============================================================
     //========================================================================================================
  PF_FileHandle pf_FileHandle;
  AttrType IX_attrType;
};


class IX_IndexScan {
public:
 IX_IndexScan();  								// Constructor
 ~IX_IndexScan(); 								// Destructor

 // for the format of "value", please see IX_IndexHandle::InsertEntry()
 RC OpenScan(IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value);

 RC GetNextEntry(RID &rid);  // Get next matching entry
 RC CloseScan();             // Terminate index scan

 RC GetFirstLeafNode( IX_IndexHandle &indexHandle,vector<void *> &leafNode,PageNum &pagenum);
 RC GetLeafNodeHavingGivenValue(IX_IndexHandle &indexHandle,vector<void *> &leafNode,PageNum &pagenum,void *value);

	vector<RID>scannedData;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
