#include "ix.h"

IX_Manager* IX_Manager::_ix_manager = 0;
RM *IX_rm;
PF_Manager *IX_pf;
const int success = 0;
//========================================================================================================
// IX_Manager methods

IX_Manager* IX_Manager::Instance()
{
if(!_ix_manager)
{
	_ix_manager = new IX_Manager();
	IX_rm = RM::Instance();
	IX_pf = PF_Manager::Instance();
	_ix_manager->CreateIndexTable();
}
return _ix_manager;
}

IX_Manager::IX_Manager   ()
{

}
IX_Manager::~IX_Manager  ()
{

}

RC IX_Manager::CreateIndex(const string tableName, const string attributeName)
{
	RC rc;
	unsigned int IX_Name_len = 0;
	unsigned int IX_tuple_size;
	void* IX_Name = malloc(50);
	void* IX_ScanValue = malloc(50);
	void* IX_DummyData = malloc(1);
	void *IX_tuple;
	IX_IndexHandle ix_handle;
	vector<Attribute> attrs;
	RID rid;
	IX_PageType indexPageType;
	//Check if table and attrname exists
	if(IX_rm->getAttributes(tableName, attrs) == success)
	{
		for(unsigned i = 0; i < attrs.size(); i++)
		{
			if(strcmp(attributeName.c_str(),attrs[i].name.c_str()) == 0)
				break;
			else if(i == attrs.size()-1)
				return 2; //Attribute doesn't exist
		}
		rc = CreateIndexName(tableName,attributeName,IX_Name,&IX_Name_len);
		if(rc != success)
			return 3; //Unable to create index name
		memcpy((char*)IX_ScanValue, &IX_Name_len, sizeof(unsigned int));
		memcpy((char*)IX_ScanValue+sizeof(unsigned int), IX_Name, IX_Name_len*sizeof(char));
		//Check if the attribute is already created - scan the Indexes table for the index name
		// Set up the iterator
		RM_ScanIterator IX_rmsi;
		string attr = "IndexName";
		vector<string> attributes;
		attributes.push_back(attr);
		rc = IX_rm->scan(tableName, attr, EQ_OP, IX_ScanValue, attributes, IX_rmsi);
		if(rc != success)
			return rc; //Error in scanning

		if(IX_rmsi.getNextTuple(rid,IX_DummyData) == RM_EOF)
		{
			//PREPARE TUPLE TO BE INSERTED INTO INDEXES TABLE
			IX_tuple = malloc(200);
			prepareIndexTuple((const char*)IX_Name ,IX_Name_len
							 ,tableName.c_str() ,(unsigned int)tableName.size()
							 ,attributeName.c_str() ,(unsigned int)attributeName.size()
							 ,(const char*)IX_Name ,IX_Name_len
							 , IX_tuple, &IX_tuple_size);

			rc = IX_pf->CreateFile((const char*)IX_Name);
			if(rc != success)
				return rc;
			//Insert the details into INDEXES table
			rc = IX_rm->insertTuple(INDEX_TABLE_NAME,(const void*)IX_tuple, rid);
			if(rc != success)
				return rc;
			IX_pf->OpenFile((const char*)IX_Name,ix_handle.pf_FileHandle);


			  //UPDATE for indexHandle.IX_attrType
			  vector<Attribute> attrs;
			  IX_rm->getAttributes(tableName, attrs);
			  for(unsigned i = 0; i < attrs.size(); i++)
			  {
				  if(strcmp(attrs[i].name.c_str(),attributeName.c_str()) == 0)
				  {
					  ix_handle.IX_attrType = attrs[i].type;
					  break;
				  }
			  }

			PageNum pagenum = 0;
			indexPageType = IndexPage;
			rc = ix_handle.IX_CreateNode(indexPageType,pagenum);
			if(rc != success)
				return rc;
//TODO: CHanged
			//Scan table and insert RID's
//			rc = IX_ScanTableInsertIndex(tableName,attributeName,ix_handle);
//			if(rc != success)
//				return rc;
		}
		else
			//INDEX ALREADY CREATED
			return -1;
		free(IX_tuple);
		IX_rmsi.close();
	}
	else
	{
		return 1; //table doesn't exist
	}
	return 0;
}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName)
{
	RC rc;
	//remove file tableName,attributeName
	unsigned int IX_Name_len = 0;
	void* IX_Name = malloc(50);
	void* IX_Compare_value = malloc(54);
	RID rid;
	vector<string> attrs;
	RM_ScanIterator IX_rmsi;
	void* key_data = malloc(100);
	attrs.push_back("IndexName");
	IX_IndexHandle IX_handle;
	rc = CreateIndexName(tableName, attributeName,IX_Name, &IX_Name_len);
	if(rc != success)
		return rc;

	rc = IX_pf->DestroyFile((const char*)IX_Name);
	if(rc != success)
		return rc;

	memcpy((char*)IX_Compare_value, &IX_Name_len, sizeof(unsigned));
	memcpy((char*)IX_Compare_value+sizeof(unsigned), IX_Name, IX_Name_len);

	rc = IX_rm->scan(INDEX_TABLE_NAME, "IndexName", EQ_OP, IX_Compare_value, attrs, IX_rmsi);
	if(rc == success)
		IX_rmsi.getNextTuple(rid, key_data);
	free(key_data);
	free(IX_Compare_value);
	IX_rmsi.close();

	rc = IX_rm->deleteTuple(INDEX_TABLE_NAME, rid);
	if(rc != success)
		return rc;
	free(IX_Name);
	return 0;
}

RC IX_Manager::OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle)
 {
	  RC rc;
	  void *indexName = malloc(100);
	  unsigned indexNameLen;
	  string indexName_str;

	  rc = CreateIndexName(tableName,attributeName,indexName,&indexNameLen);
	  if(rc != success)
		  return rc;


	  indexName_str = (char*)indexName;
	  indexName_str[indexNameLen] = '\0';
	  indexHandle.pf_FileHandle.tableName = indexName_str;
	  rc = IX_pf->OpenFile(indexName_str.c_str(),indexHandle.pf_FileHandle);
	  if(rc != success)
			return 11;

	  //UPDATE for indexHandle.IX_attrType
	  vector<Attribute> attrs;
	  IX_rm->getAttributes(tableName, attrs);
	  for(unsigned i = 0; i < attrs.size(); i++)
	  {
		  if(strcmp(attrs[i].name.c_str(),attributeName.c_str()) == 0)
		  {
			  indexHandle.IX_attrType = attrs[i].type;
			  break;
		  }
	  }
	  //UPDATE for indexHandle.IX_attrType

	  free(indexName);
	  return rc;
 }
 RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)
 {
	  RC rc;
	  rc = IX_pf->CloseFile(indexHandle.pf_FileHandle);
		if(rc != success)
			return 12;

	  return rc;
 }
//========================================================================================================

// IX_IndexHandle methods
IX_IndexHandle::IX_IndexHandle  ()
{

}
IX_IndexHandle::~IX_IndexHandle ()
{

}

//========================================================================================================
//===============================INSERT ENTRY=============================================================
//========================================================================================================

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid)
{
	void* extractedkey = malloc(4);
	const string spaces = "   ";
	memcpy(extractedkey,key,sizeof(int));

	if(!this->pf_FileHandle.isOpen())
	{
		if(this->pf_FileHandle.GetNumberOfPages()>0)
		{
			void * splittedIndexNode = malloc(8);
			int val =0;
			memcpy((char *)splittedIndexNode,&val,4);
			memcpy((char *)splittedIndexNode+4,&val,4);

			if(!this->Insert(extractedkey,rid,0,splittedIndexNode))
			{
				free(splittedIndexNode);
				free(extractedkey);

				return 0;
			}
		}
	}

	free (extractedkey);
	return 13;
}


RC IX_IndexHandle::Insert(void *key, const RID &rid,PageNum pagenum,void * splittedIndexNode)
{
	IX_PageType pageType;
	PageNum nextNodeLink;
	IX_PageType indexPageType;
	vector<void*> voidkeyrids;
	vector<void*> tempVoidkeyrids;
	vector<void*> LeftIndexNodekeyrids;
	vector<void*> rightIndexNodekeyrids;

	vector<IndexPage_TypeIntAttr>indexNodekeyrids;
	vector<LeafPage_TypeIntAttr> leafNodekeyrids;
	vector<RID>referenceNodeKeyrids;

	AttrType attrType = this->IX_attrType;
	unsigned int leftLink =0,rightLink =0, zeroValue =0;
	unsigned i;
	//reading a node from index
	this->IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,attrType,voidkeyrids);
	if(pageType == IndexPage)
	{
		if(leftLink==0)
		{
			//create 1st leaf node
			indexPageType = LeafPage;
			if(!this->IX_CreateNode(indexPageType, nextNodeLink))
			{
				this->IX_UpdatePageWithNewVectorDetails(pagenum,nextNodeLink,rightLink,pageType,attrType,voidkeyrids);
				memcpy((char *)splittedIndexNode,&zeroValue,4);
				memcpy((char *)splittedIndexNode+4,&zeroValue,4);
				Insert(key,rid,nextNodeLink,splittedIndexNode);
			}
			else
			{
				for(unsigned int i=0;i<voidkeyrids.size();i++)
				  free(voidkeyrids[i]);
				voidkeyrids.clear();

				return 14;
			}
		}
		else
		{
			if(voidkeyrids.size()==0)
				nextNodeLink = leftLink;
			for(unsigned int i=0;i<voidkeyrids.size();i++)
			{
//					if given key is less than node key return 1,
//					if given key is greater than node key return 2 ,
//					if given key is equal to node key return 3
				int comp = IX_CompareVoidKeyValues(key,voidkeyrids[i],attrType);
				switch(comp)
				{
				case 1:	if (i==0)
						{
							nextNodeLink = leftLink;
							i= voidkeyrids.size();
						}
						else
						{
							memcpy(&nextNodeLink,(char *)voidkeyrids[i-1]+4,4);
							i= voidkeyrids.size();
						}
						break;

				case 2: if(i==voidkeyrids.size()-1)
						{
							memcpy(&nextNodeLink,(char *)voidkeyrids[i]+4,4);
							i= voidkeyrids.size();
						}
						break;

				case 3: memcpy(&nextNodeLink,(char *)voidkeyrids[i]+4,4);
						i= voidkeyrids.size();
						break;
				}
			}
			Insert(key,rid,nextNodeLink,splittedIndexNode);
			if((IX_CompareVoidKeyValues((char *)splittedIndexNode,&zeroValue,attrType)==3))
			{
				for(unsigned int i=0;i<voidkeyrids.size();i++)
				  free(voidkeyrids[i]);
				voidkeyrids.clear();

				return 0;
			}
			else
			{
				if(voidkeyrids.size()<FANOUT)
				{

					void * splittedIndexNode_copy = malloc(8);
					memcpy(splittedIndexNode_copy, splittedIndexNode, 8);

					voidkeyrids.push_back(splittedIndexNode_copy);

   				    this->IX_UpdatePageWithNewVectorDetails(pagenum,leftLink,rightLink,pageType,attrType,voidkeyrids);
					memcpy((char *)splittedIndexNode,&zeroValue,4);
					memcpy((char *)splittedIndexNode+4,&zeroValue,4);

					for(unsigned int i=0;i<voidkeyrids.size();i++)
					   free(voidkeyrids[i]);
					voidkeyrids.clear();

					return 0;
				}
				else
				{
					PageNum newRightLink,newLeftLink;
					void * splittedIndexNode_copy = malloc(8);
					memcpy(splittedIndexNode_copy, splittedIndexNode, 8);

					voidkeyrids.push_back((void*)splittedIndexNode_copy);
					this->IX_SortVector(voidkeyrids,attrType, pageType);

					for(unsigned i=0;i<FANOUT/2;i++)
						LeftIndexNodekeyrids.push_back(voidkeyrids[i]);
					for(unsigned i=FANOUT/2+1;i<voidkeyrids.size();i++)//FANOUT/2 will be pushed up into the root
						rightIndexNodekeyrids.push_back(voidkeyrids[i]);

					if(pagenum==0)
					{
						//indexPageType = IndexPage;
						this->IX_CreateNode(pageType,newLeftLink);
						this->IX_UpdatePageWithNewVectorDetails(newLeftLink,leftLink,rightLink,pageType,attrType,LeftIndexNodekeyrids);


						//indexPageType = IndexPage;
						this->IX_CreateNode(pageType,newRightLink);
						int temoLeftLink;
						memcpy(&temoLeftLink,(char *)voidkeyrids[FANOUT/2]+4,4);
						this->IX_UpdatePageWithNewVectorDetails(newRightLink,temoLeftLink,rightLink,pageType,attrType,rightIndexNodekeyrids);


						void * newIndexNode = malloc(8);
						memcpy((char*)newIndexNode,(char *) voidkeyrids[FANOUT/2],4);
						memcpy((char*)newIndexNode+4,&newRightLink,4);


						tempVoidkeyrids.push_back(newIndexNode);

						this->IX_UpdatePageWithNewVectorDetails(pagenum,newLeftLink,rightLink,pageType,attrType,tempVoidkeyrids);

  					    for(unsigned int i=0;i<voidkeyrids.size();i++)
					      free(voidkeyrids[i]);
					    voidkeyrids.clear();

						for(unsigned int i=0;i<tempVoidkeyrids.size();i++)
							free(tempVoidkeyrids[i]);
						tempVoidkeyrids.clear();

						return 0;
					}
					else
					{
						//update left node
						this->IX_UpdatePageWithNewVectorDetails(pagenum,leftLink,rightLink,pageType,attrType,LeftIndexNodekeyrids);

						//create new right node
						indexPageType = IndexPage;
						this->IX_CreateNode(indexPageType,newLeftLink);


						//update right node
						int tempLeftLink;
						memcpy(&tempLeftLink,(char *)voidkeyrids[FANOUT/2]+4,4);
						this->IX_UpdatePageWithNewVectorDetails(newLeftLink,tempLeftLink,rightLink,pageType,attrType,rightIndexNodekeyrids);
						memcpy(splittedIndexNode,voidkeyrids[FANOUT/2],4);
						memcpy((char *)splittedIndexNode +4,&newLeftLink,4);

					}

					for(unsigned int i=0;i<voidkeyrids.size();i++)
					   free(voidkeyrids[i]);
					voidkeyrids.clear();

					return 0;
				}
			}
		}
		for(unsigned int i=0;i<voidkeyrids.size();i++)
		  free(voidkeyrids[i]);
        voidkeyrids.clear();

		return 0;
	}
	if(pageType == LeafPage)
	{
		//creating new leaf node value
		void * leafNode = malloc(16);
		PageNum newpagenum;
		memcpy((char *)leafNode,(char*)key,4);
		memcpy((char *)leafNode+4,&zeroValue,4);
		memcpy((char *)leafNode+8,&rid.pageNum,4);
		memcpy((char *)leafNode+12,&rid.slotNum,4);

		for(i=0;i<voidkeyrids.size();i++)
			if(IX_CompareVoidKeyValues(key,voidkeyrids[i],this->IX_attrType)==3)
				break;
		if(i<voidkeyrids.size() && voidkeyrids.size()!=0)
		{
			//adding first overflow page
			if(IX_CompareVoidKeyValues(&zeroValue,(char*)voidkeyrids[i]+4,this->IX_attrType)==3)
			{
				this->IX_CreateNode(OverflowPage,newpagenum);
				int overflowvalue;
				memcpy(&overflowvalue,(char *)voidkeyrids[i]+4,4);
				overflowvalue++;
				memcpy((char *)voidkeyrids[i]+4,&overflowvalue,4);
				RID ridTuple;
				memcpy(&ridTuple.pageNum,(char *)voidkeyrids[i]+8,4);
				memcpy(&ridTuple.slotNum,(char *)voidkeyrids[i]+12,4);
				referenceNodeKeyrids.push_back(ridTuple);

				memcpy((char *)voidkeyrids[i]+8,&newpagenum,4);
				memcpy((char *)voidkeyrids[i]+12,&zeroValue,4);

				this->IX_UpdatePageWithNewVectorDetails(pagenum,leftLink,rightLink,LeafPage,attrType,voidkeyrids);

				this->IX_Create_OverflowPage_RIDs_VoidVector(voidkeyrids,referenceNodeKeyrids);
				this->IX_UpdatePageWithNewVectorDetails(newpagenum,0,0,OverflowPage,attrType,voidkeyrids);
				memcpy((char *)splittedIndexNode,&zeroValue,4);
				memcpy((char *)splittedIndexNode+4,&zeroValue,4);
				Insert(key,rid,newpagenum,splittedIndexNode);
			}
			//if key has overflow page
			else
			{
				RID ridTuple;
				memcpy(&ridTuple.pageNum,(char *)voidkeyrids[i]+8,4);
				Insert(key,rid,ridTuple.pageNum,splittedIndexNode);
			}
		}
		else
		{
			voidkeyrids.push_back(leafNode);
			indexPageType = LeafPage;
			IX_SortVector(voidkeyrids,attrType,indexPageType);
			if(voidkeyrids.size()<=FANOUT)
			{
				this->IX_UpdatePageWithNewVectorDetails(pagenum,leftLink,rightLink,pageType,attrType,voidkeyrids);

				memcpy((char *)splittedIndexNode,&zeroValue,4);
				memcpy((char *)splittedIndexNode+4,&zeroValue,4);
			}
			else
			{
				vector<void *> LeftleafNodekeyrids,rightleafNodekeyrids,voidrightrightkeyrids;
				unsigned right_leftLink,right_rightLink;
				for(i=0;i<FANOUT/2;i++)
					LeftleafNodekeyrids.push_back(voidkeyrids[i]);
				for(i=FANOUT/2;i<(unsigned)voidkeyrids.size();i++)
					rightleafNodekeyrids.push_back(voidkeyrids[i]);

				//create new leaf node
				//indexPageType = LeafPage;
					this->IX_CreateNode(indexPageType,newpagenum);

				this->IX_UpdatePageWithNewVectorDetails(pagenum,leftLink,newpagenum,pageType,attrType,LeftleafNodekeyrids);
				//fetch pagenum->rightLink and update newpagenum->right = rightLink and rightLink->left = newpagenum
				//reading a node from right right
				if(rightLink != 0)//Meaning there is no right node to pagenum/newpagenum
				{
					this->IX_CreateVoidVectorFromPage(rightLink,&right_leftLink,&right_rightLink,&pageType,attrType,voidrightrightkeyrids);
					this->IX_UpdatePageWithNewVectorDetails(rightLink,newpagenum,right_rightLink,pageType,attrType,voidrightrightkeyrids);
				}
				this->IX_UpdatePageWithNewVectorDetails(newpagenum,pagenum,rightLink,pageType,attrType,rightleafNodekeyrids);//Wrong

				memcpy((char *)splittedIndexNode,(void*)rightleafNodekeyrids[0],4);
				memcpy((char *)splittedIndexNode+4,&newpagenum,4);

				//FREE THE ALLOCATED MEMORY

				for(i = 0; i < voidrightrightkeyrids.size(); i++)
					free(voidrightrightkeyrids[i]);
				voidrightrightkeyrids.clear();
			}
		}

		for(i = 0; i < voidkeyrids.size(); i++)
		  free(voidkeyrids[i]);
		voidkeyrids.clear();

		return 0;
	}
	if(pageType == OverflowPage)
	{
		this->IX_Create_OverflowPage_RIDs_Vector(voidkeyrids,referenceNodeKeyrids);
		if(referenceNodeKeyrids.size()<FANOUT)
		{
			referenceNodeKeyrids.push_back(rid);
			this->IX_Create_OverflowPage_RIDs_VoidVector(voidkeyrids,referenceNodeKeyrids);
			this->IX_UpdatePageWithNewVectorDetails(pagenum,leftLink,rightLink,OverflowPage,attrType,voidkeyrids);
			memcpy((char *)splittedIndexNode,&zeroValue,4);
			memcpy((char *)splittedIndexNode+4,&zeroValue,4);
		}
		else
		{
			if(rightLink !=0)
			{
				Insert(key,rid,rightLink,splittedIndexNode);
			}
			else
			{
				PageNum newpagenum;
				this->IX_CreateNode(OverflowPage,newpagenum);
				this->IX_Create_OverflowPage_RIDs_VoidVector(voidkeyrids,referenceNodeKeyrids);
				this->IX_UpdatePageWithNewVectorDetails(pagenum,0,newpagenum,OverflowPage,attrType,voidkeyrids);
				this->IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,attrType,voidkeyrids);
				Insert(key,rid,newpagenum,splittedIndexNode);
			}
		}

		for(i = 0; i < voidkeyrids.size(); i++)
			free(voidkeyrids[i]);
		voidkeyrids.clear();

		return 0;
	}

	for(i = 0; i < voidkeyrids.size(); i++)
	  free(voidkeyrids[i]);
	voidkeyrids.clear();

	return -1;
}


//========================================================================================================
//===============================INSERT ENTRY=============================================================
//========================================================================================================

//========================================================================================================
//===============================PRINT INDEX=============================================================
//========================================================================================================
RC IX_IndexHandle::printIndex()
{
	vector<unsigned int> links;
	unsigned int leftLink,rightLink;
	vector<void *>voidkeyrids;
	IX_PageType pageType;
	vector<RID>referenceNodeKeyrids;
	links.push_back(0);
	for(unsigned int i=0;i<links.size();i++)
	{
		cout<<"page num: "<<links[i]<<endl;
		this->IX_CreateVoidVectorFromPage(links[i],&leftLink,&rightLink,&pageType,this->IX_attrType,voidkeyrids);
		if(leftLink==0 && pageType==IndexPage )
			return 0;
		if(pageType==IndexPage)
		{
			vector<IndexPage_TypeIntAttr>indexNodekeyrids_int;
			vector<IndexPage_TypeRealAttr>indexNodekeyrids_real;
			vector<IndexPage_TypeVarcharAttr>indexNodekeyrids_varchar;
			if(this->IX_attrType== TypeInt)
			{
				this->IX_Create_IndexPage_TypeIntAttr_Vector(voidkeyrids,indexNodekeyrids_int);
				links.push_back(leftLink);
				cout<<"leftLink: "<<leftLink<<endl;
				for(unsigned int j=0;j<indexNodekeyrids_int.size();j++)
				{
					cout<<"||"<<indexNodekeyrids_int[j].key<<"|";
					links.push_back(indexNodekeyrids_int[j].pageid);
				}
				cout<<endl;
			}
			if(this->IX_attrType== TypeReal)
			{
				this->IX_Create_IndexPage_TypeRealAttr_Vector(voidkeyrids,indexNodekeyrids_real);
				links.push_back(leftLink);
				cout<<"leftLink: "<<leftLink<<endl;
				for(unsigned int j=0;j<indexNodekeyrids_real.size();j++)
				{
					cout<<"||"<<indexNodekeyrids_real[j].key<<"|";
					links.push_back(indexNodekeyrids_real[j].pageid);
				}
				cout<<endl;
			}
			if(this->IX_attrType== TypeVarChar)
			{
				this->IX_Create_IndexPage_TypeVarcharAttr_Vector(voidkeyrids,indexNodekeyrids_varchar);
				links.push_back(leftLink);
				cout<<"leftLink: "<<leftLink<<endl;
				for(unsigned int j=0;j<indexNodekeyrids_varchar.size();j++)
				{
					indexNodekeyrids_varchar[j].key[4] = '\0';
					cout<<"||"<<indexNodekeyrids_varchar[j].key<<"|";
					links.push_back(indexNodekeyrids_varchar[j].pageid);
				}
				cout<<endl;
			}

		}
		if(pageType==LeafPage)
		{
			vector<LeafPage_TypeIntAttr> leafNodekeyrids_int;
			vector<LeafPage_TypeRealAttr> leafNodekeyrids_Real;
			vector<LeafPage_TypeVarcharAttr> leafNodekeyrids_varchar;
			if(this->IX_attrType== TypeInt)
			{
				this->IX_Create_LeafPage_TypeIntAttr_Vector(voidkeyrids,leafNodekeyrids_int);

				cout<<"leftLink: "<<leftLink<<"rightLink"<<rightLink<<endl;
				for(unsigned int  j=0;j<leafNodekeyrids_int.size();j++)
				{
					cout<<"||"<<leafNodekeyrids_int[j].key<<"|";

					if(leafNodekeyrids_int[j].hasDuplicate!=0)
						links.push_back(leafNodekeyrids_int[j].rid.pageNum);

				}
				cout<<endl;
			}
			if(this->IX_attrType== TypeReal)
			{
				this->IX_Create_LeafPage_TypeRealAttr_Vector(voidkeyrids,leafNodekeyrids_Real);
				cout<<"leftLink: "<<leftLink<<"rightLink"<<rightLink<<endl;
				for(unsigned int  j=0;j<leafNodekeyrids_Real.size();j++)
				{
					cout<<"||"<<leafNodekeyrids_Real[j].key<<"|";

					if(leafNodekeyrids_Real[j].hasDuplicate!=0)
						links.push_back(leafNodekeyrids_Real[j].rid.pageNum);
				}
				cout<<endl;
			}
			if(this->IX_attrType== TypeVarChar)
			{
				this->IX_Create_LeafPage_TypeVarcharAttr_Vector(voidkeyrids,leafNodekeyrids_varchar);

				cout<<"leftLink: "<<leftLink<<"rightLink"<<rightLink<<endl;
				for(unsigned int j=0;j<leafNodekeyrids_varchar.size();j++)
				{
					leafNodekeyrids_varchar[j].key[4] = '\0';
					cout<<"||"<<leafNodekeyrids_varchar[j].key<<"|";

					if(leafNodekeyrids_varchar[j].hasDuplicate!=0)
						links.push_back(leafNodekeyrids_varchar[j].rid.pageNum);

				}
				cout<<endl;
			}

		}
		if(pageType==OverflowPage)
		{
			vector<RID> overflowNodeKeyRids;
			this->IX_Create_OverflowPage_RIDs_Vector(voidkeyrids,overflowNodeKeyRids);
			cout<<"overflow page rightLink"<<rightLink<<endl;
			for(unsigned int  j=0;j<overflowNodeKeyRids.size();j++)
				cout<<"||"<<overflowNodeKeyRids[j].pageNum<<"-"<<overflowNodeKeyRids[j].slotNum;
			cout<<endl;
			if(rightLink!=0)
				links.push_back(rightLink);
		}
	}
	return 0;
}



RC IX_IndexHandle::printIndex_modified()
{
	queue<PageNum> queueNodes;
	PageNum queueNode;
	PageNum currQueueNode;
	PageNum tempQueueNode;
	vector<void *>voidkeyrids;
	vector<IndexPage_TypeIntAttr> indexAttrs;
	vector<LeafPage_TypeIntAttr> leafAttrs;

	AttrType attrType= TypeInt;
	IX_PageType pageType;
	unsigned int leftLink,rightLink,i;
	unsigned int temp = 0, numOfLeaves = 0;
	queueNode = 0;
	queueNodes.push(queueNode);
	while(!queueNodes.empty())
	{
		cout << endl;
		currQueueNode = queueNodes.front();
		queueNodes.pop();
		cout<<"page num: " << currQueueNode <<endl;
		this->IX_CreateVoidVectorFromPage(currQueueNode,&leftLink,&rightLink,&pageType,attrType,voidkeyrids);
		if(pageType == IndexPage)
		{
			this->IX_Create_IndexPage_TypeIntAttr_Vector(voidkeyrids,indexAttrs);
			if(leftLink != 0)
			{
				tempQueueNode = leftLink;
				queueNodes.push(tempQueueNode);
				for(i = 0; i < indexAttrs.size(); i++)
				{
					cout << indexAttrs[i].key << " ";
					tempQueueNode = indexAttrs[i].pageid;
					queueNodes.push(tempQueueNode);
				}
				indexAttrs.clear();
			}
		}

		else
		{
			if(temp == 0)
			{
				temp++;
				cout << "-----------------------------------------------------------------------------------" << endl;
			}
			this->IX_Create_LeafPage_TypeIntAttr_Vector(voidkeyrids,leafAttrs);
			for(i = 0; i < leafAttrs.size(); i++)
			{
				cout << leafAttrs[i].key << " ";
				numOfLeaves++;
			}
			indexAttrs.clear();
		}
	}
	cout << endl;
	cout << "#numOfLeaves: " << numOfLeaves << endl;;
	return 0;
}


//========================================================================================================
//===============================PRINT INDEX=============================================================
//========================================================================================================

//========================================================================================================
//===============================DELETE ENTRY=============================================================
//========================================================================================================
RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)
{
	int oldchildentryDummy;
	unsigned recLen;
	PageNum nodepointerDummy;
	void* extractedkey = malloc(4);
	const string spaces = "   ";
	nodepointerDummy = 0;
	oldchildentryDummy = NULLENTRY;
	unsigned searchFound = 0;
	unsigned leftRootPointerPageNum,rightRootPointerPageNum,leftChildPointerPageNum,rightChildPointerPageNum;
	unsigned i;
	IX_PageType ix_root_pageType;
	IX_PageType ix_child_pageType;
	AttrType attrType = this->IX_attrType;
	vector<void*> root_voidKeyrids;
	vector<void*> child_voidKeyrids;

	if(this->IX_attrType == TypeVarChar)
	{
		memcpy(&recLen, key, sizeof(int));
		if(recLen >= sizeof(int))
			memcpy((char*)extractedkey,(char*)key+sizeof(int),sizeof(int));
		else
		{
			memcpy((char*)extractedkey,(char*)key+sizeof(int),recLen);
			memcpy((char*)extractedkey,spaces.c_str(),sizeof(int)-recLen);
		}
	}
	else
	{
		memcpy((char*)extractedkey,(char*)key,sizeof(int));
	}
	if(!this->pf_FileHandle.isOpen())
	{
		if(this->pf_FileHandle.GetNumberOfPages()>1)	// Minimum of 1 Root and 1 Leaf
		{
			if((this->IX_DeleteSearch(extractedkey,rid,0,&nodepointerDummy, &oldchildentryDummy, &searchFound) == success) && (searchFound == 1))
			{
				//if the root has 0 nodes and the nextlevelnode is IndexPage, copy that index to root and delete thenextlevelnode
				//remove oldchildentry from Index Node
				if(!IX_CreateVoidVectorFromPage(0, &leftRootPointerPageNum, &rightRootPointerPageNum, &ix_root_pageType, attrType, root_voidKeyrids))
				{
					if(root_voidKeyrids.size() == 0) //Root has only left pointer
					{
						if(!IX_CreateVoidVectorFromPage(leftRootPointerPageNum
								, &leftChildPointerPageNum, &rightChildPointerPageNum, &ix_child_pageType, attrType, child_voidKeyrids))
						{

							if(ix_child_pageType == IndexPage)
							{
								void* IX_buffer = malloc(PF_PAGE_SIZE);
								memset(IX_buffer, 0x0, PF_PAGE_SIZE);
								
								//COPY TO ROOT
								IX_CreateBufferPage(0, leftChildPointerPageNum,rightRootPointerPageNum,IX_buffer, ix_root_pageType, child_voidKeyrids);

								this->pf_FileHandle.WritePage(0,IX_buffer);
								//DELETE CHILD
								for(i = 0; i < child_voidKeyrids.size(); i++)
									free(child_voidKeyrids[i]);
								child_voidKeyrids.clear();
								
								IX_CreateBufferPage(0, 0,0,IX_buffer, ix_root_pageType, child_voidKeyrids);
								this->pf_FileHandle.WritePage(leftRootPointerPageNum,IX_buffer);
								
								free(IX_buffer);
							}
							else if((ix_child_pageType == LeafPage) && (child_voidKeyrids.size() == 0))
							{
								vector<void*> emptyKeyRids;
								void* IX_buffer_empty = malloc(PF_PAGE_SIZE);
								memset(IX_buffer_empty,0x0,PF_PAGE_SIZE);
								
								//Change root left pointer to zero
								IX_CreateBufferPage(0, 0,0,IX_buffer_empty, ix_root_pageType, emptyKeyRids);
								this->pf_FileHandle.WritePage(0,IX_buffer_empty);
								
								free(IX_buffer_empty);
							}

							for(i = 0; i < child_voidKeyrids.size(); i++)
								free(child_voidKeyrids[i]);
							child_voidKeyrids.clear();
						}
					}
					for(i = 0; i < root_voidKeyrids.size(); i++)
						free(root_voidKeyrids[i]);
					root_voidKeyrids.clear();
				}
				
				free(extractedkey);
				return 0;
			}
			else
			{
				free(extractedkey);
				return 21; //KEY NOT FOUND
			}
		}
		else
		{
			free(extractedkey);
			return 22; //No keys Exist
		}
	}
	
	free(extractedkey);
	return 23; //File not open
}

RC IX_IndexHandle::IX_DeleteSearch(void *key, const RID &rid,PageNum parentpointer,PageNum *nodepointer,int *oldchildentry,unsigned *searchFound)
{
//	void* currPageBuffer = malloc(PF_PAGE_SIZE);
//	memset(currPageBuffer,0x0,PF_PAGE_SIZE);

	unsigned leftPointerPageNum,rightPointerPageNum;
	int compareValue;
	PageNum prevPointerPageNum,nextLevelnodepointer;
	unsigned iValue;
	unsigned isBreak = 0;
	RC rc;
	PageNum overflowPageNum;
	IX_PageType ix_pageType;
	AttrType attrType = this->IX_attrType;
	vector<void*> voidKeyrids;
	PageNum leftSiblingPointer,rightSiblingPointer;
	int numOfKeysInLeftSibling,numOfKeysInRightSibling;
	//if(!this->pf_FileHandle.ReadPage(*nodepointer,currPageBuffer))
	if(*nodepointer < this->pf_FileHandle.GetNumberOfPages())
	{
		//free(currPageBuffer);
		
		if(!IX_CreateVoidVectorFromPage(*nodepointer, &leftPointerPageNum, &rightPointerPageNum, &ix_pageType, attrType, voidKeyrids))
		{
			if (*nodepointer == leftPointerPageNum)
			{
				for(unsigned i = 0; i < voidKeyrids.size(); i++)
					free(voidKeyrids[i]);
				voidKeyrids.clear();
				
				return 25;//No nodes to delete
            }
			if(ix_pageType == IndexPage)
			{
				prevPointerPageNum = leftPointerPageNum;
				isBreak = 0;
				for(unsigned i = 0; i < voidKeyrids.size(); i++)
				{
					compareValue = IX_CompareVoidKeyValues(key, voidKeyrids[i],attrType);
					switch (compareValue)
					{
						case 1://key < voidKeyrids
						{
							nextLevelnodepointer = prevPointerPageNum;
							isBreak = 1;
							break;
						}
						case 2://key > voidKeyrids
						{
							memcpy(&prevPointerPageNum,(char*)voidKeyrids[i]+sizeof(int), sizeof(int));
							break;
						}
						case 3://equal
						{
							memcpy(&nextLevelnodepointer,(char*)voidKeyrids[i]+sizeof(int), sizeof(int));
							isBreak = 1;
							break;
						}
					}
					if(isBreak == 1)
					{
						iValue = i;
						break;
					}
				}
				if((isBreak != 1) && voidKeyrids.size() >= 1)//child Key greater than last key in the parent node
				{
					memcpy(&nextLevelnodepointer,(char*)voidKeyrids[voidKeyrids.size()-1]+sizeof(int), sizeof(int));
					//*nodepointer = nextLevelnodepointer;
				}
				else if(voidKeyrids.size() == 0)
				{
					nextLevelnodepointer = leftPointerPageNum;
				}
				//else
				//{
					if((IX_DeleteSearch(key,rid,*nodepointer,&nextLevelnodepointer,oldchildentry,searchFound) == success) && (*searchFound == 1))
					{
						if(*oldchildentry == NULLENTRY) //usual case: child not deleted
						{
							for(unsigned i = 0; i < voidKeyrids.size(); i++)
								free(voidKeyrids[i]);
							voidKeyrids.clear();
							
							return 0;
						}
						else
						{
							//remove oldchildentry from Index Node
							iValue = 0;
							for(unsigned i = 0; i < voidKeyrids.size(); i++)
							{
								if(memcmp(oldchildentry, voidKeyrids[i],sizeof(int)) == 0)
								{
									iValue = i;
									break;
								}
							}

							voidKeyrids.erase(voidKeyrids.begin()+iValue);
							//check for underflow
							if((voidKeyrids.size() >= FANOUT/2)	|| (*nodepointer == 0))//No problem: write back to file
							{
								void* IX_buffer = malloc(PF_PAGE_SIZE);
								memset(IX_buffer, 0x0, PF_PAGE_SIZE);
								
								IX_CreateBufferPage(*nodepointer, leftPointerPageNum,rightPointerPageNum,IX_buffer, ix_pageType, voidKeyrids);
								this->pf_FileHandle.WritePage(*nodepointer,IX_buffer);
								
								free(IX_buffer);
								*oldchildentry = NULLENTRY;
					
								for(unsigned i = 0; i < voidKeyrids.size(); i++)
									free(voidKeyrids[i]);
								voidKeyrids.clear();

								return 0;
							}
							else //Underflow
							{
								void* IX_buffer = malloc(PF_PAGE_SIZE);
								memset(IX_buffer, 0x0, PF_PAGE_SIZE);

								IX_CreateBufferPage(*nodepointer, leftPointerPageNum,rightPointerPageNum,IX_buffer, ix_pageType, voidKeyrids);
								this->pf_FileHandle.WritePage(*nodepointer,IX_buffer);

								free(IX_buffer);

								//get sibling using parentpointer
								//load parent page, find previous and next nodes pointers of key,rid
								//pass parent page num, *nodepointer, find left sibling page num,key, #keysinpage, right sibling page num,key, #keysinpage
								//above method should send NULLENTRY for pages if *nodepointer is the leftmost or rightmost in the node
								//else if #keys in right page  + curr page >= fanout -> IX_Redistribute
								//else IX_Merge with left page if no left page, IX_Merge with right page
								IX_GetSiblingDetails(key, rid,parentpointer, *nodepointer,&leftSiblingPointer
											,&numOfKeysInLeftSibling,&rightSiblingPointer,&numOfKeysInRightSibling);

								//if #keys in left page  + curr page >= fanout -> IX_Redistribute
								if((numOfKeysInLeftSibling != NULLENTRY) && (numOfKeysInRightSibling != NULLENTRY))
								{
									if((numOfKeysInLeftSibling+voidKeyrids.size()) >= FANOUT)
									{
										//"IX_Redistribute FROM LEFT: "
										rc = IX_Redistribute(parentpointer,leftSiblingPointer,*nodepointer);
										*oldchildentry = NULLENTRY;

										for(unsigned i = 0; i < voidKeyrids.size(); i++)
											free(voidKeyrids[i]);
										voidKeyrids.clear();

										return rc;
									}
									else if((numOfKeysInRightSibling+voidKeyrids.size()) >= FANOUT)
									{
										//"IX_Redistribute FROM RIGHT"<< endl;
										rc = IX_Redistribute(parentpointer,*nodepointer,rightSiblingPointer);
										*oldchildentry = NULLENTRY;

										for(unsigned i = 0; i < voidKeyrids.size(); i++)
											free(voidKeyrids[i]);
										voidKeyrids.clear();

										return rc;
									}
									else
									{
										//cout << "IX_Merge WITH LEFT - PULL SPLITTING KEY FROM PARENT DOWN INTO NODE FROM LEFT"<< endl;
										rc = IX_Merge(parentpointer,leftSiblingPointer,*nodepointer,oldchildentry);

										for(unsigned i = 0; i < voidKeyrids.size(); i++)
											free(voidKeyrids[i]);
										voidKeyrids.clear();

										return rc;
									}
								}
								else if(numOfKeysInLeftSibling == NULLENTRY)
								{
									if((numOfKeysInRightSibling+voidKeyrids.size()) >= FANOUT)
									{
										//cout << "IX_Redistribute FROM RIGHT"<< endl;
										rc = IX_Redistribute(parentpointer,*nodepointer,rightSiblingPointer);
										*oldchildentry = NULLENTRY;

										for(unsigned i = 0; i < voidKeyrids.size(); i++)
											free(voidKeyrids[i]);
										voidKeyrids.clear();

										return rc;
									}
									else
									{
										//cout << "IX_Merge WITH RIGHT - PULL SPLITTING KEY FROM PARENT DOWN INTO NODE FROM LEFT"<< endl;
										rc = IX_Merge(parentpointer,*nodepointer,rightSiblingPointer,oldchildentry);

										for(unsigned i = 0; i < voidKeyrids.size(); i++)
											free(voidKeyrids[i]);
										voidKeyrids.clear();

										return rc;
									}
								}
								else if(numOfKeysInRightSibling == NULLENTRY)
								{
									if((numOfKeysInLeftSibling+voidKeyrids.size()) >= FANOUT)
									{
										//cout << "IX_Redistribute FROM LEFT"<< endl;
										rc = IX_Redistribute(parentpointer,leftSiblingPointer,*nodepointer);
										*oldchildentry = NULLENTRY;

										for(unsigned i = 0; i < voidKeyrids.size(); i++)
											free(voidKeyrids[i]);
										voidKeyrids.clear();

										return rc;
									}
									else
									{
										//cout << "IX_Merge WITH LEFT - PULL SPLITTING KEY FROM PARENT DOWN INTO NODE FROM LEFT"<< endl;
										rc = IX_Merge(parentpointer,leftSiblingPointer,*nodepointer,oldchildentry);

										for(unsigned i = 0; i < voidKeyrids.size(); i++)
											free(voidKeyrids[i]);
										voidKeyrids.clear();

										return rc;
									}
								}

							}
						}
					}
					else {
						for(unsigned i = 0; i < voidKeyrids.size(); i++)
							free(voidKeyrids[i]);
						voidKeyrids.clear();

						return 21;//Key not found
					}
				//}
			}

			else if(ix_pageType == LeafPage) //ADJUST SIBLING POINTERS
			{
				prevPointerPageNum = leftPointerPageNum;
				isBreak = 0;
				for(unsigned i = 0; i < voidKeyrids.size(); i++)
				{
					compareValue = IX_CompareVoidKeyValues(key, voidKeyrids[i],attrType);//Compare Key values
					if(compareValue == 3)
					{
						unsigned hasDuplicates = 0;
						compareValue = memcmp((char*)voidKeyrids[i]+sizeof(int),&hasDuplicates, sizeof(int));
						if(compareValue == 0)//Check if it has overflow...if no continue, else
						{
							compareValue = IX_CompareVoidRIDValues(rid,voidKeyrids[i]);//Compare Rid values
							if(compareValue == 0)
							{
								isBreak = 1;
							}
						}
						else
						{
							//copy overflowPageNum from vector
							memcpy(&overflowPageNum,(char*)voidKeyrids[i]+2*sizeof(int),sizeof(int));
							compareValue = IX_CompareVoidRIDValuesFromOverFlow(rid,overflowPageNum);//Compare Rid values
							if(compareValue == 0)
							{
								*searchFound = 1;
								return 0;//successfully deleted entry in OverFlow page. No further action needed.
							}
							else if(compareValue == 1)//Delete the key
							{
								isBreak = 1;
							}
							else
								return 21;//match not found
						}
					}
					if(isBreak == 1)
					{
						iValue = i;
						break;
					}
				}
				if(isBreak == 1)
				{
					*searchFound = 1;
					for(unsigned i = 0; i < voidKeyrids.size(); i++)
					{
						if(IX_ComparePageNumValuesInIndexNode(*oldchildentry, voidKeyrids[i]) ==0)
						{
							iValue = i;
							break;
						}
					}
					voidKeyrids.erase(voidKeyrids.begin()+iValue);
					//check for underflow
					if(voidKeyrids.size() >= FANOUT/2)	//No problem: write back to file
					{
						void* IX_buffer = malloc(PF_PAGE_SIZE);
						memset(IX_buffer,0x0,PF_PAGE_SIZE);
												
						rc = IX_CreateBufferPage(*nodepointer, leftPointerPageNum,rightPointerPageNum,IX_buffer, ix_pageType, voidKeyrids);
						this->pf_FileHandle.WritePage(*nodepointer,IX_buffer);
						free(IX_buffer);
						*oldchildentry = NULLENTRY; //CHANGED NOV17

						for(unsigned i = 0; i < voidKeyrids.size(); i++)
							free(voidKeyrids[i]);
						voidKeyrids.clear();

						return rc;
					}
					else //Underflow
					{
						void* IX_buffer = malloc(PF_PAGE_SIZE);
						memset(IX_buffer,0x0,PF_PAGE_SIZE);
						
						IX_CreateBufferPage(*nodepointer, leftPointerPageNum,rightPointerPageNum,IX_buffer, ix_pageType, voidKeyrids);
						this->pf_FileHandle.WritePage(*nodepointer,IX_buffer);
						free(IX_buffer);
						//get sibling using parentpointer
						//load parent page, find previous and next nodes pointers of key,rid
						//pass parent page num, *nodepointer, find left sibling page num,key, #keysinpage, right sibling page num,key, #keysinpage
						//above method should send NULLENTRY for pages if *nodepointer is the leftmost or rightmost in the node
						//else if #keys in right page  + curr page >= fanout -> IX_Redistribute
						//else IX_Merge with left page if no left page, IX_Merge with right page
						IX_GetSiblingDetails(key, rid,parentpointer, *nodepointer,&leftSiblingPointer
									,&numOfKeysInLeftSibling,&rightSiblingPointer,&numOfKeysInRightSibling);
						//if #keys in left page  + curr page >= fanout -> IX_Redistribute
						if((numOfKeysInLeftSibling == NULLENTRY) && (numOfKeysInRightSibling == NULLENTRY))//Should be the only nchild or root
						{
							for(unsigned i = 0; i < voidKeyrids.size(); i++)
								free(voidKeyrids[i]);
							voidKeyrids.clear();
						}
						else if((numOfKeysInLeftSibling != NULLENTRY) && (numOfKeysInRightSibling != NULLENTRY))
						{
							if((numOfKeysInLeftSibling+voidKeyrids.size()) >= FANOUT)
							{
								//cout << "leaf IX_Redistribute FROM LEFT "<< endl;
								rc = IX_Redistribute(parentpointer,leftSiblingPointer,*nodepointer);
								*oldchildentry = NULLENTRY;

								for(unsigned i = 0; i < voidKeyrids.size(); i++)
									free(voidKeyrids[i]);
								voidKeyrids.clear();

								return rc;
							}
							else if((numOfKeysInRightSibling+voidKeyrids.size()) >= FANOUT)
							{
								//cout << "leaf IX_Redistribute FROM RIGHT"<< endl;
								rc = IX_Redistribute(parentpointer,*nodepointer,rightSiblingPointer);
								*oldchildentry = NULLENTRY;

								for(unsigned i = 0; i < voidKeyrids.size(); i++)
									free(voidKeyrids[i]);
								voidKeyrids.clear();

								return rc;
							}
							else
							{
								//cout << "leaf IX_Merge WITH LEFT"<< endl;
								rc = IX_Merge(parentpointer,leftSiblingPointer,*nodepointer,oldchildentry);

								for(unsigned i = 0; i < voidKeyrids.size(); i++)
									free(voidKeyrids[i]);
								voidKeyrids.clear();

								return rc;
							}
						}
						else if(numOfKeysInLeftSibling == NULLENTRY)
						{
							if((numOfKeysInRightSibling+voidKeyrids.size()) >= FANOUT)
							{
								//cout << "leaf IX_Redistribute FROM RIGHT"<< endl;
								rc = IX_Redistribute(parentpointer,*nodepointer,rightSiblingPointer);
								*oldchildentry = NULLENTRY;

								for(unsigned i = 0; i < voidKeyrids.size(); i++)
									free(voidKeyrids[i]);
								voidKeyrids.clear();

								return rc;
							}
							else
							{
								//cout << "leaf IX_Merge WITH RIGHT"<< endl;
								rc = IX_Merge(parentpointer,*nodepointer,rightSiblingPointer,oldchildentry);

								for(unsigned i = 0; i < voidKeyrids.size(); i++)
									free(voidKeyrids[i]);
								voidKeyrids.clear();

								return rc;
							}
						}
						else if(numOfKeysInRightSibling == NULLENTRY)
						{
							if((numOfKeysInLeftSibling+voidKeyrids.size()) >= FANOUT)
							{
								//cout << "leaf IX_Redistribute FROM LEFT"<< endl;
								rc = IX_Redistribute(parentpointer,leftSiblingPointer,*nodepointer);
								*oldchildentry = NULLENTRY;

								for(unsigned i = 0; i < voidKeyrids.size(); i++)
									free(voidKeyrids[i]);
								voidKeyrids.clear();

								return rc;
							}
							else
							{
								//cout << "leaf IX_Merge WITH LEFT"<< endl;
								rc = IX_Merge(parentpointer,leftSiblingPointer,*nodepointer,oldchildentry);

								for(unsigned i = 0; i < voidKeyrids.size(); i++)
									free(voidKeyrids[i]);
								voidKeyrids.clear();

								return rc;
							}
						}

					}
				}
				else //Search not found
				{
					*searchFound = 0;

					for(unsigned i = 0; i < voidKeyrids.size(); i++)
						free(voidKeyrids[i]);
					voidKeyrids.clear();
					return 21;
				}
			}
		}
		else {
			for(unsigned i = 0; i < voidKeyrids.size(); i++)
				free(voidKeyrids[i]);
			voidKeyrids.clear();
			return 21; //Key not found
		}
	}
	else
	{
		//free(currPageBuffer);
		return 24; //Error reading page
	}
	return 0;
}

unsigned IX_IndexHandle::IX_CompareVoidKeyValues(void* givenKey, void* pageKey, AttrType attrType)
{
       //unsigned result = 0;
       if(attrType == TypeInt)
       {//Convert givenKey andPageKey to int -> compare -> return
              int givenKeyInt, pageKeyInt;
              memcpy(&givenKeyInt,givenKey,sizeof(int));
              memcpy(&pageKeyInt,pageKey,sizeof(int));
              if(givenKeyInt < pageKeyInt)
                     return 1;
              if(givenKeyInt > pageKeyInt)
                     return 2;
              if(givenKeyInt == pageKeyInt)
                     return 3;
       }

       else if(attrType == TypeReal)
       {//Convert givenKey andPageKey to real -> compare -> return
              float givenKeyFloat, pageKeyFloat;
              memcpy(&givenKeyFloat,givenKey,sizeof(int));
              memcpy(&pageKeyFloat,pageKey,sizeof(int));
              if(givenKeyFloat < pageKeyFloat)
                     return 1;
              if(givenKeyFloat > pageKeyFloat)
                     return 2;
              if(givenKeyFloat == pageKeyFloat)
                     return 3;
       }

       else if(attrType == TypeVarChar)
       {//Convert givenKey andPageKey to varchar -> compare -> return
              string givenKeyString, pageKeyString;
              memcpy((void*)givenKeyString.c_str(),givenKey,sizeof(int));
              memcpy((void*)pageKeyString.c_str(),pageKey,sizeof(int));
              givenKeyString[4] = '\0';
              pageKeyString[4] = '\0';
              if(givenKeyString.compare(pageKeyString) < 0)
                     return 1;
              if(givenKeyString.compare(pageKeyString) > 0)
                     return 2;
              if(givenKeyString.compare(pageKeyString) == 0)
                     return 3;
       }
       return 0;
}

unsigned IX_IndexHandle::IX_CompareVoidRIDValues(const RID &rid,void* pageKey)
{
	if((memcmp(&rid.pageNum,(char*)pageKey+2*sizeof(int),sizeof(int))  == 0) && (memcmp(&rid.slotNum,(char*)pageKey+3*sizeof(int),sizeof(int))  == 0))
		return 0;
	else
		return 1;
}

unsigned IX_IndexHandle::IX_ComparePageNumValuesInIndexNode(int oldchildentry, void* pageKey)
{
	if(memcmp(&oldchildentry,(char*)pageKey+sizeof(int),sizeof(int)) == 0)
		return 0;
	else
		return 1;
}

RC IX_IndexHandle::IX_GetnumOfKeysInNode(PageNum nodepointer,int *numOfKeysInNode)
{
	unsigned leftPointerPageNum,rightPointerPageNum;
	IX_PageType ix_pageType;
	AttrType attrType = this->IX_attrType;
	vector<void*> voidKeyrids;
	if(IX_CreateVoidVectorFromPage(nodepointer, &leftPointerPageNum, &rightPointerPageNum, &ix_pageType, attrType, voidKeyrids))
	{
		*numOfKeysInNode = NULLENTRY;
		return 1;
	}
	*numOfKeysInNode = voidKeyrids.size();
	if(*numOfKeysInNode == 0)
		*numOfKeysInNode = NULLENTRY;
	for(unsigned i = 0; i < voidKeyrids.size(); i++)
		free(voidKeyrids[i]);
	voidKeyrids.clear();
	return 0;
}
RC IX_IndexHandle::IX_GetSiblingDetails(void *key, const RID &rid
		   	   	   	   	   	   	   	   ,PageNum parentpointer,PageNum nodepointer
		   	   	   	   	   	   	   	   ,PageNum *leftSiblingPointer,int *numOfKeysInLeftSibling
		   	   	   	   	   	   	   	   ,PageNum *rightSiblingPointer,int *numOfKeysInRightSibling)
{
	unsigned leftPointerPageNum,rightPointerPageNum,compareValue;
	PageNum leftNodepointer,rightNodepointer;
	IX_PageType ix_pageType;
	AttrType attrType = this->IX_attrType;
	vector<void*> voidKeyrids;
	void *comparekey = malloc(sizeof(int));
	IX_CreateVoidVectorFromPage(parentpointer, &leftPointerPageNum, &rightPointerPageNum, &ix_pageType, attrType, voidKeyrids);
	if(voidKeyrids.size() == 0)	//This is the only child - can't do anything
	{
		*leftSiblingPointer = 0;
		*numOfKeysInLeftSibling = NULLENTRY;
		*rightSiblingPointer = 0;
		*numOfKeysInRightSibling = NULLENTRY;
	}
	else
	{
		memcpy(comparekey,&nodepointer,sizeof(int));
		//first check with the leftPointerPageNum
		compareValue = IX_CompareVoidKeyValues(comparekey, &leftPointerPageNum,attrType);
		if(compareValue == 3)
		{
			*leftSiblingPointer = 0;
			*numOfKeysInLeftSibling = NULLENTRY;
			memcpy(&rightNodepointer, (char*)voidKeyrids[0]+sizeof(int),sizeof(int));
			*rightSiblingPointer = rightNodepointer;
			IX_GetnumOfKeysInNode(*rightSiblingPointer,numOfKeysInRightSibling);
		}
		for(unsigned i = 0; i < voidKeyrids.size(); i++)
		{
			compareValue = IX_CompareVoidKeyValues(comparekey, (char*)voidKeyrids[i]+sizeof(int),attrType);
			if(compareValue == 3)
			{
				if(i == 0) //nodepointer is the left most node
				{
					*leftSiblingPointer = leftPointerPageNum;
					IX_GetnumOfKeysInNode(*leftSiblingPointer,numOfKeysInLeftSibling);
					//*numOfKeysInLeftSibling = NULLENTRY;
					if(i+1 < voidKeyrids.size())
					{
						memcpy(&rightNodepointer, (char*)voidKeyrids[i+1]+sizeof(int),sizeof(int));
						*rightSiblingPointer = rightNodepointer;
						IX_GetnumOfKeysInNode(*rightSiblingPointer,numOfKeysInRightSibling);
					}
					else
					{
						*rightSiblingPointer = 0;
						*numOfKeysInRightSibling = NULLENTRY;
					}
				}
				else if(i == voidKeyrids.size()-1)//nodepointer is the right most node
				{
					*rightSiblingPointer = 0;
					*numOfKeysInRightSibling = NULLENTRY;
					memcpy(&leftNodepointer, (char*)voidKeyrids[i-1]+sizeof(int),sizeof(int));
					*leftSiblingPointer = leftNodepointer;
					IX_GetnumOfKeysInNode(*leftSiblingPointer,numOfKeysInLeftSibling);
				}
				else if((i > 0) && (i < voidKeyrids.size()-1))
				{
					memcpy(&leftNodepointer, (char*)voidKeyrids[i-1]+sizeof(int),sizeof(int));
					memcpy(&rightNodepointer, (char*)voidKeyrids[i+1]+sizeof(int),sizeof(int));
					*leftSiblingPointer = leftNodepointer;
					*rightSiblingPointer = rightNodepointer;
					IX_GetnumOfKeysInNode(*leftSiblingPointer,numOfKeysInLeftSibling);
					IX_GetnumOfKeysInNode(*rightSiblingPointer,numOfKeysInRightSibling);
				}
				break;
			}
		}
	}

	free(comparekey);

	for(unsigned i = 0; i < voidKeyrids.size(); i++)
		free(voidKeyrids[i]);
	voidKeyrids.clear();

	return 0;
}

RC IX_IndexHandle::IX_Redistribute(PageNum parentPagePointer,PageNum leftPagePointer,PageNum rightPagePointer)
{
	//IX_Redistribute FROM LEFT
	//open two pages and fetch the vectors
	//IX_Merge two vectors and sort
	//send first floor(half) into left vector, other into right vector
	//write back two pages
	//update parent
	PageNum leftPageLeftLink,leftPageRightLink,rightPageLeftLink,rightPageRightLink,parentPageLeftLink,parentPageRightLink;
	IX_PageType ix_parentPageType,ix_childrenPageType;
	AttrType attrType = this->IX_attrType;
	vector<void*> voidKeyridsLeft;
	vector<void*> voidKeyridsRight;
	vector<void*> voidKeyridsNewLeft;
	vector<void*> voidKeyridsNewRight;
	vector<void*> voidKeyridsParent;
	vector<void*> voidKeyridsNewParent;
	vector<void*> all_nodes;

	void* updateParentKey = malloc(4);
	void* updateParentKeyPrev = malloc(4);
	void* updateRightNodeLeftLink = malloc(4);
	unsigned i, rightPageLeftLink_updated;
	unsigned size, divideNode;
	IX_CreateVoidVectorFromPage(leftPagePointer, &leftPageLeftLink, &leftPageRightLink, &ix_childrenPageType, attrType, voidKeyridsLeft);
	IX_CreateVoidVectorFromPage(rightPagePointer, &rightPageLeftLink, &rightPageRightLink, &ix_childrenPageType, attrType, voidKeyridsRight);
	IX_CreateVoidVectorFromPage(parentPagePointer, &parentPageLeftLink, &parentPageRightLink, &ix_parentPageType, attrType, voidKeyridsParent);
	RC rc;

	for(i = 0; i < voidKeyridsLeft.size(); i++)
		all_nodes.push_back(voidKeyridsLeft[i]);
	for(i = 0; i < voidKeyridsRight.size(); i++)
		all_nodes.push_back(voidKeyridsRight[i]);
	for(i = 0; i < voidKeyridsParent.size(); i++)
		all_nodes.push_back(voidKeyridsParent[i]);

	if(ix_childrenPageType == IndexPage)
	{
		//PUSH PARENT
		for(i = 0; i < voidKeyridsParent.size(); i++) //Its an index node: storing parents right node link
		{
				if(memcmp((char*)voidKeyridsParent[i]+sizeof(int),&rightPagePointer,sizeof(int)) == 0)
				{
					memcpy((char*)updateParentKeyPrev,(char*)voidKeyridsParent[i],sizeof(int));
					memcpy((char*)voidKeyridsParent[i] + sizeof(int), &rightPageLeftLink,sizeof(int));//Modifying parents next to prev rights left
					voidKeyridsLeft.push_back(voidKeyridsParent[i]);
					break;
				}
		}

		//PUSH RIGHT NODE
		for(i = 0; i < voidKeyridsRight.size(); i++)
			voidKeyridsLeft.push_back(voidKeyridsRight[i]);


		//Pick the median for sending it to parent:
		size = (unsigned)voidKeyridsLeft.size();
		divideNode = (size%2) ? ((size-1)/2): ((size/2)-1);
		//Populate new left
		for(i = 0; i < divideNode ; i++ )
			voidKeyridsNewLeft.push_back(voidKeyridsLeft[i]);

		//Populate new right
		for(i = divideNode+1; i < voidKeyridsLeft.size(); i++)
			voidKeyridsNewRight.push_back(voidKeyridsLeft[i]);

		//Update new right's left link
		memcpy(&rightPageLeftLink_updated,(char*)voidKeyridsLeft[divideNode]+sizeof(int), sizeof(int));

		//Update parent's pointer
		IX_CreateVoidVectorFromPage(parentPagePointer, &parentPageLeftLink, &parentPageRightLink, &ix_parentPageType, attrType, voidKeyridsNewParent);
		memcpy((char*)updateParentKey,(char*)voidKeyridsLeft[divideNode], sizeof(int));
		for(i = 0; i < voidKeyridsParent.size(); i++) //Nust need to modify the key...pointer is same
		{
				if(memcmp((char*)voidKeyridsNewParent[i],(char*)updateParentKeyPrev,sizeof(int)) == 0)
				{
					memcpy((char*)voidKeyridsNewParent[i],(char*)updateParentKey,sizeof(int));
					break;
				}
		}

		IX_UpdatePageWithNewVectorDetails(leftPagePointer,leftPageLeftLink,leftPageRightLink,ix_childrenPageType, attrType, voidKeyridsNewLeft);
		IX_UpdatePageWithNewVectorDetails(rightPagePointer,rightPageLeftLink_updated,rightPageRightLink,ix_childrenPageType, attrType, voidKeyridsNewRight);
		IX_UpdatePageWithNewVectorDetails(parentPagePointer,parentPageLeftLink,parentPageRightLink,ix_parentPageType, attrType, voidKeyridsNewParent);
	}
	else
	{
		for(i = 0; i < voidKeyridsParent.size(); i++) //Its an index node
		{
				if(memcmp((char*)voidKeyridsParent[i]+sizeof(int),&rightPagePointer,sizeof(int)) == 0)
				{
					memcpy((char*)updateParentKeyPrev,(char*)voidKeyridsParent[i],sizeof(int));
					//voidKeyridsLeft.push_back(voidKeyridsParent[i]);
					break;
				}
		}

		for(i = 0; i < voidKeyridsRight.size(); i++)
			voidKeyridsLeft.push_back(voidKeyridsRight[i]);
		//IX_Merged vector will be in order - no need of sorting
		for(i = 0; i < voidKeyridsLeft.size()/2 ; i++ )
			voidKeyridsNewLeft.push_back(voidKeyridsLeft[i]);

		for(i = voidKeyridsLeft.size()/2; i < voidKeyridsLeft.size(); i++)
			voidKeyridsNewRight.push_back(voidKeyridsLeft[i]);
		//Update Left node
		IX_UpdatePageWithNewVectorDetails(leftPagePointer,leftPageLeftLink,leftPageRightLink,ix_childrenPageType, attrType, voidKeyridsNewLeft);

		//Update Right node

		IX_UpdatePageWithNewVectorDetails(rightPagePointer,rightPageLeftLink,rightPageRightLink,ix_childrenPageType, attrType, voidKeyridsNewRight);

		//Update the Parent node
		memcpy((char*)updateParentKey,(char*)voidKeyridsNewRight[0],sizeof(int));
		IX_CreateVoidVectorFromPage(parentPagePointer, &parentPageLeftLink, &parentPageRightLink, &ix_parentPageType, attrType, voidKeyridsNewParent);

		for(i = 0; i < voidKeyridsParent.size(); i++) //Its an index node
		{
				//if(memcmp((char*)voidKeyridsParent[i]+sizeof(int),&rightPagePointer,sizeof(int)) == 0)
				if(memcmp((char*)voidKeyridsNewParent[i],(char*)updateParentKeyPrev,sizeof(int)) == 0)
				{
					memcpy((char*)voidKeyridsNewParent[i],(char*)updateParentKey,sizeof(int));
					break;
				}
		}

		rc = IX_UpdatePageWithNewVectorDetails(parentPagePointer,parentPageLeftLink,parentPageRightLink,ix_parentPageType, attrType, voidKeyridsNewParent);

	}
	for(i = 0; i < voidKeyridsNewParent.size(); i++)
		free(voidKeyridsNewParent[i]);
	for(i = 0; i < all_nodes.size(); i++)
		free(all_nodes[i]);

    all_nodes.clear();
	voidKeyridsLeft.clear();
	voidKeyridsNewLeft.clear();
	voidKeyridsRight.clear();
	voidKeyridsNewRight.clear();
	voidKeyridsParent.clear();
	voidKeyridsNewParent.clear();

	free(updateParentKey);
	free(updateParentKeyPrev);
	free(updateRightNodeLeftLink);

	if(rc != success)
		return 27;//Error in Redistribute for delete
	return 0;
}

RC IX_IndexHandle::IX_Merge(PageNum parentPagePointer,PageNum leftPagePointer,PageNum rightPagePointer,int *oldchildentry)
{
	//Pass left and right pages
	//IX_Merge everything to left page
	//update *oldchildentry to key in parent which has to be deleted...that is the keyrid pointing to right page
	//Fetch Right->Right for updating siblings
	PageNum leftPageLeftLink,leftPageRightLink,rightPageLeftLink,rightPageRightLink;
	PageNum parentPageLeftLink,parentPageRightLink,RightRightPage_LeftLink,RightRightPage_RightLink;
	IX_PageType ix_children_pageType;
	IX_PageType ix_parent_pageType;
	AttrType attrType = this->IX_attrType;
	vector<void*> voidKeyridsLeft;
	vector<void*> voidKeyridsRight;
	vector<void*> voidKeyridsParent;
	vector<void*> voidKeyridsRightRight;
	vector<void*> all_nodes;
	RC rc;

	unsigned i;
	IX_CreateVoidVectorFromPage(leftPagePointer, &leftPageLeftLink, &leftPageRightLink, &ix_children_pageType, attrType, voidKeyridsLeft);
	IX_CreateVoidVectorFromPage(rightPagePointer, &rightPageLeftLink, &rightPageRightLink, &ix_children_pageType, attrType, voidKeyridsRight);
	//Fetch the parent page and check the node whose right pointer matches with rightPagePointer
	IX_CreateVoidVectorFromPage(parentPagePointer, &parentPageLeftLink, &parentPageRightLink, &ix_parent_pageType, attrType, voidKeyridsParent);

	for(i = 0; i < voidKeyridsLeft.size(); i++)
		all_nodes.push_back(voidKeyridsLeft[i]);
	for(i = 0; i < voidKeyridsRight.size(); i++)
		all_nodes.push_back(voidKeyridsRight[i]);
	for(i = 0; i < voidKeyridsParent.size(); i++)
		all_nodes.push_back(voidKeyridsParent[i]);

	for(i = 0; i < voidKeyridsParent.size(); i++)
	{
			if(memcmp((char*)voidKeyridsParent[i]+sizeof(int),&rightPagePointer,sizeof(int)) == 0)
			{
				//PULL DOWN
				if(ix_children_pageType == IndexPage)//When pulling, should copy key value, page pointer should be left link of right sibling
				{
					voidKeyridsLeft.push_back(voidKeyridsParent[i]);
					memcpy((char*)voidKeyridsLeft[voidKeyridsLeft.size()-1]+sizeof(int),&rightPageLeftLink,sizeof(int));
				}
				memcpy(oldchildentry,(char*)voidKeyridsParent[i],sizeof(int));
				break;
			}
	}
	for(i = 0; i < voidKeyridsRight.size(); i++)
	{
		voidKeyridsLeft.push_back(voidKeyridsRight[i]);
	}

	if((ix_children_pageType == LeafPage) && (rightPageRightLink != 0)) //Update RIght->Right Node
	{
		IX_CreateVoidVectorFromPage(rightPageRightLink, &RightRightPage_LeftLink, &RightRightPage_RightLink, &ix_children_pageType, attrType, voidKeyridsRightRight);
		RightRightPage_LeftLink = rightPageLeftLink;
		IX_UpdatePageWithNewVectorDetails(rightPageRightLink,RightRightPage_LeftLink,RightRightPage_RightLink,ix_children_pageType, attrType, voidKeyridsRightRight);

		for(i = 0; i < voidKeyridsRightRight.size(); i++)
			free(voidKeyridsRightRight[i]);
		voidKeyridsRightRight.clear();
	}

	//Update Left node
	if(ix_children_pageType == LeafPage)
			leftPageRightLink = rightPageRightLink;
	IX_UpdatePageWithNewVectorDetails(leftPagePointer,leftPageLeftLink,leftPageRightLink,ix_children_pageType, attrType, voidKeyridsLeft);

	for(i = 0; i < all_nodes.size(); i++)
		free(all_nodes[i]);

    all_nodes.clear();
	voidKeyridsLeft.clear();
	voidKeyridsParent.clear();
	voidKeyridsRight.clear();

	rc = IX_UpdatePageWithNewVectorDetails(rightPagePointer,0,0,ix_children_pageType, attrType, voidKeyridsRight);
	if(rc!=success)
		return 26;//Error in Merge

	return 0;
}



RC IX_IndexHandle::IX_CompareVoidRIDValuesFromOverFlow(const RID &rid,PageNum OverflowPageNum)
{
	//Return 0 if the page has given rid
	//else return 1
	AttrType attrType = TypeInt;
	IX_PageType ix_pageType = OverflowPage;
	PageNum leftOverFlowPage, rightOverFlowPage;
	unsigned i;
	vector<void*> voidKeyrids;
	vector<RID> RIDs;
	RC returnValue = 0;
	unsigned matchFound = 0;

	IX_CreateVoidVectorFromPage(OverflowPageNum, &leftOverFlowPage, &rightOverFlowPage, &ix_pageType, attrType, voidKeyrids);
	while(1)
	{
		RIDs.clear();
		IX_Create_OverflowPage_RIDs_Vector(voidKeyrids, RIDs);
		for(i = 0; i<voidKeyrids.size();i++)
			free(voidKeyrids[i]);
		voidKeyrids.clear();

		for(i = 0; i<RIDs.size();i++)
		{
			if((rid.pageNum == RIDs[i].pageNum) && (rid.slotNum == RIDs[i].slotNum))
			{
				matchFound= 1;
				break;
			}
		}
		if(matchFound == 1)
		{
			RIDs.erase(RIDs.begin()+i);
			if((RIDs.size() == 0) && (leftOverFlowPage==0) && (rightOverFlowPage==0))
				returnValue = 1;//Delete Key from B+ Tree index
			IX_Create_OverflowPage_RIDs_VoidVector(voidKeyrids,RIDs);
			IX_UpdatePageWithNewVectorDetails(OverflowPageNum,leftOverFlowPage,rightOverFlowPage,ix_pageType, attrType, voidKeyrids);
			break;
		}
		else
		{
			OverflowPageNum = rightOverFlowPage;
			if(rightOverFlowPage!=0)
				IX_CreateVoidVectorFromPage(OverflowPageNum, &leftOverFlowPage, &rightOverFlowPage, &ix_pageType, attrType, voidKeyrids);
			else
				break;
		}
	}
	RIDs.clear();
	for(i = 0; i<voidKeyrids.size();i++)
		free(voidKeyrids[i]);
	if(matchFound != 1)
		returnValue = 2; //Match not found
	return returnValue;
}

//========================================================================================================
//===============================DELETE ENTRY=============================================================
//========================================================================================================

//IX_IndexScan methods
IX_IndexScan::IX_IndexScan()
{

}
IX_IndexScan::~IX_IndexScan()
{

}

RC IX_IndexScan::OpenScan(IX_IndexHandle &indexHandle,CompOp compOp, void *value)
{
	RC rc;
	vector<void *> leafNode;
	PageNum pagenum =0;
	unsigned int leftLink,rightLink = 1;
	IX_PageType pageType;
	RID rid;
	vector<RID>tempScannedData;
	if(!indexHandle.pf_FileHandle.isOpen())
	{
		indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,indexHandle.IX_attrType,leafNode);
		if(leftLink == 0)
			return 0;
		if(compOp == NO_OP || compOp == NE_OP)
		{
			pagenum =0;
			rc = this->GetFirstLeafNode(indexHandle,leafNode,pagenum);
			if(rc != success)
				return 15;

			while(rightLink !=0)
			{
				indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,indexHandle.IX_attrType,leafNode);
				pagenum = rightLink;
				for(unsigned int i=0;i<leafNode.size();i++)
				{
					if(compOp == NE_OP)
					{
						int comp  = indexHandle.IX_CompareVoidKeyValues(value,leafNode[i],indexHandle.IX_attrType);
						if(comp == 3)
						{}
						else
						{
							unsigned int overflow,overflowRightLink;
							PageNum newpagenum;
							memcpy(&overflow,(char*)leafNode[i]+4,4);
							if(overflow!=0)
							{
								memcpy(&overflowRightLink,(char*)leafNode[i]+8,4);
								vector<RID>overflowKeyrids;
								vector<void *> refnode;
								newpagenum = overflowRightLink;
								while(overflowRightLink!=0)
								{
									indexHandle.IX_CreateVoidVectorFromPage(newpagenum,&leftLink,&overflowRightLink,&pageType,indexHandle.IX_attrType,refnode);
									indexHandle.IX_Create_OverflowPage_RIDs_Vector(refnode,overflowKeyrids);
									for(unsigned int j =0;j<overflowKeyrids.size();j++)
										tempScannedData.push_back(overflowKeyrids[j]);
									newpagenum = overflowRightLink;

								}

							}
							else
							{
								memcpy(&rid.pageNum,(char*)leafNode[i]+8,4);
								memcpy(&rid.slotNum,(char*)leafNode[i]+12,4);
								tempScannedData.push_back(rid);
							}
						}
					}
					else
					{
						unsigned int overflow,overflowRightLink;
						PageNum newpagenum;
						memcpy(&overflow,(char*)leafNode[i]+4,4);
						if(overflow!=0)
						{
							memcpy(&overflowRightLink,(char*)leafNode[i]+8,4);
							vector<RID>overflowKeyrids;
							vector<void *> refnode;
							newpagenum = overflowRightLink;
							while(overflowRightLink!=0)
							{
								indexHandle.IX_CreateVoidVectorFromPage(newpagenum,&leftLink,&overflowRightLink,&pageType,indexHandle.IX_attrType,refnode);
								indexHandle.IX_Create_OverflowPage_RIDs_Vector(refnode,overflowKeyrids);
								for(unsigned int j =0;j<overflowKeyrids.size();j++)
									tempScannedData.push_back(overflowKeyrids[j]);
								newpagenum = overflowRightLink;
							}
						}
						else
						{
							memcpy(&rid.pageNum,(char*)leafNode[i]+8,4);
							memcpy(&rid.slotNum,(char*)leafNode[i]+12,4);
							tempScannedData.push_back(rid);
						}
					}
				}
			}
		}
		if(compOp == EQ_OP)
		{
			pagenum =0;
			rc = this->GetLeafNodeHavingGivenValue(indexHandle,leafNode,pagenum,value);
			if(rc != success)
				return 15;
			for(unsigned int i =0;i<leafNode.size();i++)
			{
				if(indexHandle.IX_CompareVoidKeyValues(value,leafNode[i],indexHandle.IX_attrType)==3)
				{
					unsigned int overflow,overflowRightLink;
					PageNum newpagenum;
					memcpy(&overflow,(char*)leafNode[i]+4,4);
					if(overflow!=0)
					{
						memcpy(&overflowRightLink,(char*)leafNode[i]+8,4);
						vector<RID>overflowKeyrids;
						vector<void *> refnode;
						newpagenum = overflowRightLink;
						while(overflowRightLink!=0)
						{
							indexHandle.IX_CreateVoidVectorFromPage(newpagenum,&leftLink,&overflowRightLink,&pageType,indexHandle.IX_attrType,refnode);
							indexHandle.IX_Create_OverflowPage_RIDs_Vector(refnode,overflowKeyrids);
							for(unsigned int j =0;j<overflowKeyrids.size();j++)
								tempScannedData.push_back(overflowKeyrids[j]);
							newpagenum = overflowRightLink;
						}
					}
					else
					{
						memcpy(&rid.pageNum,(char*)leafNode[i]+8,4);
						memcpy(&rid.slotNum,(char*)leafNode[i]+12,4);
						tempScannedData.push_back(rid);
					}
				}
			}
		}
		if(compOp == LT_OP||compOp==LE_OP)
		{
			pagenum =0;
			bool flag  = true;
			this->GetFirstLeafNode(indexHandle,leafNode,pagenum);
			while(flag)
			{
				indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,indexHandle.IX_attrType,leafNode);
				pagenum = rightLink;
				for(unsigned i=0;i<leafNode.size();i++)
				{
					int comp  = indexHandle.IX_CompareVoidKeyValues(leafNode[i],value,indexHandle.IX_attrType);
					if(comp ==1)
					{
						unsigned int overflow,overflowRightLink;
						PageNum newpagenum;
						memcpy(&overflow,(char*)leafNode[i]+4,4);
						if(overflow!=0)
						{
							memcpy(&overflowRightLink,(char*)leafNode[i]+8,4);
							vector<RID>overflowKeyrids;
							vector<void *> refnode;
							newpagenum = overflowRightLink;
							while(overflowRightLink!=0)
							{
								indexHandle.IX_CreateVoidVectorFromPage(newpagenum,&leftLink,&overflowRightLink,&pageType,indexHandle.IX_attrType,refnode);
								indexHandle.IX_Create_OverflowPage_RIDs_Vector(refnode,overflowKeyrids);
								for(unsigned int j =0;j<overflowKeyrids.size();j++)
									tempScannedData.push_back(overflowKeyrids[j]);
								newpagenum = overflowRightLink;
							}
						}
						else
						{
							memcpy(&rid.pageNum,(char*)leafNode[i]+8,4);
							memcpy(&rid.slotNum,(char*)leafNode[i]+12,4);
							tempScannedData.push_back(rid);
						}
					}
					if(comp ==2)
					{
						flag = false;
						i=leafNode.size();
					}
					if(comp == 3)
					{
						if(compOp==LE_OP)
						{
							unsigned int overflow,overflowRightLink;
							PageNum newpagenum;
							memcpy(&overflow,(char*)leafNode[i]+4,4);
							if(overflow!=0)
							{
								memcpy(&overflowRightLink,(char*)leafNode[i]+8,4);
								vector<RID>overflowKeyrids;
								vector<void *> refnode;
								newpagenum = overflowRightLink;
								while(overflowRightLink!=0)
								{
									indexHandle.IX_CreateVoidVectorFromPage(newpagenum,&leftLink,&overflowRightLink,&pageType,indexHandle.IX_attrType,refnode);
									indexHandle.IX_Create_OverflowPage_RIDs_Vector(refnode,overflowKeyrids);
									for(unsigned int j =0;j<overflowKeyrids.size();j++)
										tempScannedData.push_back(overflowKeyrids[j]);
									newpagenum = overflowRightLink;
								}
							}
							else
							{
								memcpy(&rid.pageNum,(char*)leafNode[i]+8,4);
								memcpy(&rid.slotNum,(char*)leafNode[i]+12,4);
								tempScannedData.push_back(rid);
							}
						}
						flag = false;
						i=leafNode.size();
					}
				}
			}
		}
		if(compOp == GT_OP||compOp==GE_OP)
		{
			pagenum =0;
			rc = this->GetLeafNodeHavingGivenValue(indexHandle,leafNode,pagenum,value);
			if(rc!=success)
				return 16;
			while(rightLink !=0)
			{
				indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,indexHandle.IX_attrType,leafNode);
				pagenum = rightLink;
				for(unsigned int i=0;i<leafNode.size();i++)
				{
					int comp  = indexHandle.IX_CompareVoidKeyValues(leafNode[i],value,indexHandle.IX_attrType);

					if(comp==3 && compOp==GE_OP )
					{
						unsigned int overflow,overflowRightLink;
						PageNum newpagenum;
						memcpy(&overflow,(char*)leafNode[i]+4,4);
						if(overflow!=0)
						{
							memcpy(&overflowRightLink,(char*)leafNode[i]+8,4);
							vector<RID>overflowKeyrids;
							vector<void *> refnode;
							newpagenum = overflowRightLink;
							while(overflowRightLink!=0)
							{
								indexHandle.IX_CreateVoidVectorFromPage(newpagenum,&leftLink,&overflowRightLink,&pageType,indexHandle.IX_attrType,refnode);
								indexHandle.IX_Create_OverflowPage_RIDs_Vector(refnode,overflowKeyrids);
								for(unsigned int j =0;j<overflowKeyrids.size();j++)
									tempScannedData.push_back(overflowKeyrids[j]);
								newpagenum = overflowRightLink;
							}
						}
						else
						{
							memcpy(&rid.pageNum,(char*)leafNode[i]+8,4);
							memcpy(&rid.slotNum,(char*)leafNode[i]+12,4);
							tempScannedData.push_back(rid);
						}
					}
					if(comp == 2)
					{
						unsigned int overflow,overflowRightLink;
						PageNum newpagenum;
						memcpy(&overflow,(char*)leafNode[i]+4,4);
						if(overflow!=0)
						{
							memcpy(&overflowRightLink,(char*)leafNode[i]+8,4);
							vector<RID>overflowKeyrids;
							vector<void *> refnode;
							newpagenum = overflowRightLink;
							while(overflowRightLink!=0)
							{
								indexHandle.IX_CreateVoidVectorFromPage(newpagenum,&leftLink,&overflowRightLink,&pageType,indexHandle.IX_attrType,refnode);
								indexHandle.IX_Create_OverflowPage_RIDs_Vector(refnode,overflowKeyrids);
								for(unsigned int j =0;j<overflowKeyrids.size();j++)
									tempScannedData.push_back(overflowKeyrids[j]);
								newpagenum = overflowRightLink;
							}
						}
						else
						{
							memcpy(&rid.pageNum,(char*)leafNode[i]+8,4);
							memcpy(&rid.slotNum,(char*)leafNode[i]+12,4);
							tempScannedData.push_back(rid);
						}
					}
				}
			}
		}
		while(!tempScannedData.empty())
		{
			this->scannedData.push_back(tempScannedData[tempScannedData.size()-1]);
			tempScannedData.pop_back();
		}
		return 0;
	}
	return -1;
}

RC IX_IndexScan::GetLeafNodeHavingGivenValue(IX_IndexHandle &indexHandle,vector<void *> &node,PageNum &pagenum,void *value)
{
	RC rc;
	unsigned int leftLink,rightLink;
	IX_PageType pageType;
	rc = indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,indexHandle.IX_attrType,node);
	if(rc!=success)
		return -1;
	if(leftLink == 0)
		return 0;
	else if(pagenum == 0 && node.size() == 0 && leftLink !=0)
	{
		pagenum = leftLink;
		indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,indexHandle.IX_attrType,node);
		return 0;
	}
	else
	{
		while(pageType!=LeafPage)
		{
			for(unsigned int i=0;i<node.size();i++)
			{
	//			if given key is less than node key return 1,
	//			if given key is greater than node key return 2 ,
	//			if given key is equal to node key return 3
				int comp = indexHandle.IX_CompareVoidKeyValues(value,node[i],indexHandle.IX_attrType);
				switch(comp)
				{
				case 1:	if (i==0)
						{
							pagenum = leftLink;
							i= node.size();
						}
						else
						{
							memcpy(&pagenum,(char *)node[i-1]+4,4);
							i= node.size();
						}
						break;

				case 2: if(i==node.size()-1)
						{
							memcpy(&pagenum,(char *)node[i]+4,4);
							i= node.size();
						}
						break;

				case 3: memcpy(&pagenum,(char *)node[i]+4,4);
						i= node.size();
						break;
				}
			}
			rc = indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,indexHandle.IX_attrType,node);
			if(rc!=success)
				return -1;
		}
		return 0;
	}
}
RC IX_IndexScan::GetFirstLeafNode(IX_IndexHandle &indexHandle,vector<void *> &leafNode,PageNum &pagenum )
{
	RC rc;
	unsigned int leftLink,rightLink;;
	AttrType attrType = TypeInt;
	IX_PageType pageType;

	rc = indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,attrType,leafNode);
	if(rc !=success)
		return -1;
	while(pageType!=LeafPage)
	{
		pagenum = leftLink;
		indexHandle.IX_CreateVoidVectorFromPage(pagenum,&leftLink,&rightLink,&pageType,attrType,leafNode);
	}
	return 0;
}

RC IX_IndexScan::GetNextEntry(RID &rid)
{
	if(!scannedData.empty())
	{
		rid.pageNum = this->scannedData[scannedData.size()-1].pageNum;
		rid.slotNum = this->scannedData[scannedData.size()-1].slotNum;
		scannedData.pop_back();
		return 0;
	}
  return IX_EOF;
}
RC IX_IndexScan::CloseScan()
{
	while(scannedData.size()!=0)
		scannedData.pop_back();
  return 0;
}
//========================================================================================================
//========================================================================================================

void IX_PrintError (RC rc)
{
	switch (rc)
	{
	case 1:
		cout << "Table doesn't exist" << endl;
		break;
	case 2:
		cout << "Attribute doesn't exist" << endl;
		break;
	case 3:
		cout << "Unable to create Index name" << endl;
		break;
	case 11:
		cout << "Failed to open Index" << endl;
		break;
	case 12:
		cout << "Failed to close Index" << endl;
		break;
	case 13:
		cout << "Failed to insert entry into Index" << endl;
		break;
	case 14:
		cout << "Failed to create new node" << endl;
		break;
	case 15:
		cout << "Failed to read First leaf node" << endl;
		break;
	case 16:
		cout << "Failed to read leaf node with given key" << endl;
		break;
	case 21:
		cout << "Key not found for deletion" << endl;
		break;
	case 22:
		cout << "No keys Exist" << endl;
		break;
	case 23:
		cout << "File not open for deletion" << endl;
		break;
	case 24:
		cout << "Error reading page from Index file" << endl;
		break;
	case 25:
		cout << "Empty index - nothing to delete" << endl;
		break;
	case 26:
		cout << "Error in Merge in DeleteEntry" << endl;
		break;
	case 27:
		cout << "Error in Redistribute in DeleteEntry" << endl;
		break;
	default:
		cout << "Undefined error" << endl;
		break;
	}
	return;
}

//========================================================================================================
//=================================INTERNAL METHODS========IX MANAGER===============================================
//========================================================================================================
void  IX_Manager::CreateIndexTable()
{
	const string tablename;
	vector<Attribute> attrs;

	Attribute attr;

	attr.name = "IndexName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	attrs.push_back(attr);

	attr.name = "TableName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)20;
	attrs.push_back(attr);

	attr.name = "attrName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)20;
	attrs.push_back(attr);

	attr.name = "IndexFileName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	attrs.push_back(attr);

	//RC rc = IX_rm->createTable(INDEX_TABLE_NAME, attrs);
	IX_rm->createTable(INDEX_TABLE_NAME, attrs);

	return;
}

RC IX_Manager::CreateIndexName(const string tableName, const string attributeName, void* indexName, unsigned int* indexName_size)
{
  unsigned int offset = 0;

  memcpy((char*)indexName, "IX", 2*sizeof(char));
  offset += 2*sizeof(char);

  memcpy((char*)indexName+offset, "_", sizeof(char));
  offset += 1*sizeof(char);

  memcpy((char*)indexName+offset,tableName.c_str(), tableName.size());
  offset += tableName.size();

  memcpy((char*)indexName+offset, "_", sizeof(char));
  offset += 1*sizeof(char);

  memcpy((char*)indexName+offset,attributeName.c_str(), attributeName.size());
  offset += attributeName.size();

  memcpy((char*)indexName+offset, "\0", sizeof(char));

  *indexName_size = offset;

  return 0;
}

RC IX_Manager::prepareIndexTuple(const char *indexName , const unsigned int indexNameLength
							 , const char *tableName , const unsigned int tableNameLength
							 , const char *attributeName , const unsigned int attributeNameLength
							 , const char *indexfileName , const unsigned int indexfileNameLength
							 , void *tuple, unsigned int *tuple_size)
{
	unsigned int offset = 0;


	memcpy((char *)tuple + offset, &(indexNameLength), sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, indexName, indexNameLength);
	offset += indexNameLength;

	memcpy((char *)tuple + offset, &(tableNameLength), sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, tableName, tableNameLength);
	offset += tableNameLength;

	memcpy((char *)tuple + offset, &(attributeNameLength), sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, attributeName, attributeNameLength);
	offset += attributeNameLength;

	memcpy((char *)tuple + offset, &(indexfileNameLength), sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, indexfileName, indexfileNameLength);
	offset += indexfileNameLength;

	memcpy((char *)tuple + offset, "\0" , sizeof(char));
	*tuple_size = offset;

	return 0;
}


RC IX_Manager::IX_PrepareEmptyRootOrIndexTuple(void *tuple)
{
	unsigned int IX_Value = 0;
	memcpy((char*)tuple,&IX_Value,sizeof(int));
	memcpy((char*)tuple+sizeof(int),&IX_Value,sizeof(int));
	return 0;
}

RC IX_Manager::IX_ScanTableInsertIndex(const string tableName, const string attributeName,IX_IndexHandle & IX_handle)
{
	vector<string> attrs;
	RM_ScanIterator IX_rmsi;
	RID rid;
	void* key_data = malloc(100);
	attrs.push_back(attributeName);
	if(!IX_handle.pf_FileHandle.isOpen())
	{
		if(!IX_rm->scan(tableName, "", NO_OP, "", attrs, IX_rmsi))
		{
			while(IX_rmsi.getNextTuple(rid, key_data) != RM_EOF)
			{
				IX_handle.InsertEntry(key_data, rid);
			}
		}
	}

	free(key_data);
	IX_rmsi.close();
	return 0;
}


//========================================================================================================
//========================================INTERNAL METHODS ===========IX HANDLE===========================
//========================================================================================================

RC IX_IndexHandle::IX_UpdatePageWithNewVectorDetails(unsigned pageNum,unsigned leftPointerPageNum,unsigned rightPointerPageNum,IX_PageType ix_pageType, AttrType attrType, vector<void*>& keyrids)
{
	RC rc;
//	PF_FileHandle IX_fileHande;
	void* IX_buffer = malloc(PF_PAGE_SIZE);
	memset(IX_buffer,0x0,PF_PAGE_SIZE);
	//CALL SORT VECTOR
	IX_SortVector(keyrids,attrType,ix_pageType);

	rc = IX_CreateBufferPage(pageNum,leftPointerPageNum,rightPointerPageNum,IX_buffer, ix_pageType, keyrids);
	if(rc != success)
	{
		free(IX_buffer);
		return rc;
	}
	//WRITE PAGE BACK TO FILE
	rc = pf_FileHandle.WritePage(pageNum,IX_buffer);
	if(rc != success)
	{
		free(IX_buffer);
		return rc;
	}

	free(IX_buffer);
	return 0;
}




bool IX_sort_IndexPage_TypeIntAttr (IndexPage_TypeIntAttr i,IndexPage_TypeIntAttr j)
{
	return (i.key < j.key);
}
bool IX_sort_IndexPage_TypeRealAttr (IndexPage_TypeRealAttr i,IndexPage_TypeRealAttr j)
{
	return (i.key < j.key);
}
bool IX_sort_IndexPage_TypeVarcharAttr (IndexPage_TypeVarcharAttr i,IndexPage_TypeVarcharAttr j)
{
	string s1 = i.key;
	s1[4] = '\0';
	string s2 = j.key;
	s2[4] = '\0';
	if(s1.compare(s2) < 0)
		return true;
	else
		return false;
}
bool IX_sort_LeafPage_TypeIntAttr (LeafPage_TypeIntAttr i,LeafPage_TypeIntAttr j)
{
	return (i.key < j.key);
}
bool IX_sort_LeafPage_TypeRealAttr (LeafPage_TypeRealAttr i,LeafPage_TypeRealAttr j)
{
	return (i.key < j.key);
}
bool IX_sort_LeafPage_TypeVarcharAttr (LeafPage_TypeVarcharAttr i,LeafPage_TypeVarcharAttr j)
{
	string s1 = i.key;
	s1[4] = '\0';
	string s2 = j.key;
	s2[4] = '\0';
	if(s1.compare(s2) < 0)
		return true;
	else
		return false;
}

void IX_IndexHandle::IX_SortVector(vector<void*>& keyrids,AttrType attrType,IX_PageType ix_pageType)
{
	unsigned i;
	unsigned size;
	vector<void*> copyKeyRids;
	void* copykeyrid;
	
	if((ix_pageType == IndexPage) || (ix_pageType == OverflowPage))
		size = 8;
	else
		size = 16;
		
	for(i = 0; i < keyrids.size(); i++)
	{
	    copykeyrid = malloc(size);
	    memcpy(copykeyrid,keyrids[i],size);
	    copyKeyRids.push_back(copykeyrid);
	}
	
	switch(ix_pageType)
	{
		case IndexPage:
		{
			switch(attrType)
			{
				case TypeInt:
				{//CHANGED
					vector<IndexPage_TypeIntAttr> tempKeyRIDs;
					IX_Create_IndexPage_TypeIntAttr_Vector(copyKeyRids, tempKeyRIDs);
					sort(tempKeyRIDs.begin(),tempKeyRIDs.end(),IX_sort_IndexPage_TypeIntAttr);
					for(i = 0; i < copyKeyRids.size(); i++)
						free(copyKeyRids[i]);
					copyKeyRids.clear();
					IX_Create_IndexPage_TypeIntAttr_VoidVector(copyKeyRids, tempKeyRIDs);
					break;
				}
				case TypeReal:
				{
					vector<IndexPage_TypeRealAttr> tempKeyRIDs;
					IX_Create_IndexPage_TypeRealAttr_Vector(copyKeyRids, tempKeyRIDs);
					sort(tempKeyRIDs.begin(),tempKeyRIDs.end(),IX_sort_IndexPage_TypeRealAttr);
					for(i = 0; i < copyKeyRids.size(); i++)
						free(copyKeyRids[i]);
					copyKeyRids.clear();
					IX_Create_IndexPage_TypeRealAttr_VoidVector(copyKeyRids, tempKeyRIDs);
					break;
				}
				case TypeVarChar:
				{
					vector<IndexPage_TypeVarcharAttr> tempKeyRIDs;
					IX_Create_IndexPage_TypeVarcharAttr_Vector(copyKeyRids, tempKeyRIDs);
					sort(tempKeyRIDs.begin(),tempKeyRIDs.end(),IX_sort_IndexPage_TypeVarcharAttr);
					for(i = 0; i < copyKeyRids.size(); i++)
						free(copyKeyRids[i]);
					copyKeyRids.clear();
					IX_Create_IndexPage_TypeVarcharAttr_VoidVector(copyKeyRids, tempKeyRIDs);
					break;
				}
			}
			break;
		}
		case LeafPage:
		{
			switch(attrType)
			{
				case TypeInt:
				{
					vector<LeafPage_TypeIntAttr> tempKeyRIDs;
					IX_Create_LeafPage_TypeIntAttr_Vector(copyKeyRids, tempKeyRIDs);
					sort(tempKeyRIDs.begin(),tempKeyRIDs.end(),IX_sort_LeafPage_TypeIntAttr);
					for(i = 0; i < copyKeyRids.size(); i++)
						free(copyKeyRids[i]);
					copyKeyRids.clear();
					IX_Create_LeafPage_TypeIntAttr_VoidVector(copyKeyRids, tempKeyRIDs);
					break;
				}
				case TypeReal:
				{
					vector<LeafPage_TypeRealAttr> tempKeyRIDs;
					IX_Create_LeafPage_TypeRealAttr_Vector(copyKeyRids, tempKeyRIDs);
					sort(tempKeyRIDs.begin(),tempKeyRIDs.end(),IX_sort_LeafPage_TypeRealAttr);
					for(i = 0; i < copyKeyRids.size(); i++)
						free(copyKeyRids[i]);
					copyKeyRids.clear();
					IX_Create_LeafPage_TypeRealAttr_VoidVector(copyKeyRids, tempKeyRIDs);
					break;
				}
				case TypeVarChar:
				{
					vector<LeafPage_TypeVarcharAttr> tempKeyRIDs;
					IX_Create_LeafPage_TypeVarcharAttr_Vector(copyKeyRids, tempKeyRIDs);
					sort(tempKeyRIDs.begin(),tempKeyRIDs.end(),IX_sort_LeafPage_TypeVarcharAttr);
					for(i = 0; i < copyKeyRids.size(); i++)
						free(copyKeyRids[i]);
					copyKeyRids.clear();
					
					IX_Create_LeafPage_TypeVarcharAttr_VoidVector(copyKeyRids, tempKeyRIDs);
					break;
				}
			}
			break;
		}
		case OverflowPage:
			break;
	}
	for(i = 0; i < keyrids.size(); i++)
	{
	    memcpy(keyrids[i],copyKeyRids[i],size);
	    free(copyKeyRids[i]);
	}	
	copyKeyRids.clear();
	
	return;
}


RC IX_IndexHandle::IX_CreateBufferPage(unsigned pageNum, unsigned leftPointerPageNum,unsigned rightPointerPageNum,void* IX_buffer, IX_PageType ix_pageType, vector<void*>& keyrids)
{
	RC rc;
	rc = IX_InitializeBuffer(IX_buffer,ix_pageType,leftPointerPageNum,rightPointerPageNum);
	if(rc != success)
		return rc;
	unsigned int recLen = 0;
	IX_CalculateRecLen(&recLen,ix_pageType);

	for(unsigned i = 0; i < keyrids.size(); i++)
		IX_insertTupleIntoBuffer(pageNum, IX_buffer, keyrids[i], recLen);
	return 0;
}
RC IX_IndexHandle::IX_InitializeBuffer(void* IX_buffer,IX_PageType ix_pageType,unsigned leftPointerPageNum,unsigned rightPointerPageNum)
{

	unsigned int offset = 0;
	void *IX_tuple = malloc(12);
	//Update first 8 bytes if IX_PageType = IndexPage
	IX_PreparePageTypeDetailsSlot(IX_tuple,ix_pageType,leftPointerPageNum);
	memcpy((char*)IX_buffer+offset,IX_tuple,8);

	if(ix_pageType == LeafPage ||ix_pageType == OverflowPage )
	{
		memcpy((char*)IX_buffer+8,&rightPointerPageNum,4);
	}
	//Update last 12 bytes
	if(ix_pageType == LeafPage||ix_pageType == OverflowPage)
		IX_PreparePageDetailsSlot(IX_tuple, 0, 12,PF_PAGE_SIZE - 12 - 12 );
	else
		IX_PreparePageDetailsSlot(IX_tuple, 0, 8,PF_PAGE_SIZE - 12 - 8 );

	offset = PF_PAGE_SIZE-12;
	memcpy((char*)IX_buffer+offset,IX_tuple,12);
	free(IX_tuple);
	return 0;
}


void IX_IndexHandle::IX_CalculateRecLen(unsigned int* recLen,IX_PageType ix_pageType)
{
	switch(ix_pageType)
	{
	case IndexPage:
		*recLen = 8;//key(4) + pageId(4) Bytes
		break;
	case LeafPage:
		*recLen = 16;//key(4) + hasDuplicate(4) + RID(8) Bytes
		break;
	case OverflowPage:
		*recLen = 8;//RID(8) Bytes
		break;
	}
	return;
}

void IX_IndexHandle::IX_insertTupleIntoBuffer(unsigned pageNum, void* IX_buffer, void* IX_tuple, unsigned recLen)
{
	unsigned NumOfRecs, RecOffset, freeSpace,offset;
	void* IX_pageDetailstuple = malloc(12);

	IX_CalculateNumRecsOffsetFreeSpaceInPage(IX_buffer, &NumOfRecs, &RecOffset, &freeSpace);

	//Copying the record
	memcpy((char*)IX_buffer+RecOffset,(const char*)IX_tuple,recLen);

	//Insert slot details of the record //pageid, slot#, offset
	IX_PreparePageDetailsSlot(IX_pageDetailstuple,pageNum, NumOfRecs , RecOffset);
	offset = PF_PAGE_SIZE-12-(NumOfRecs+1)*12;
	memcpy((char*)IX_buffer+offset,IX_pageDetailstuple,12);

	//Update last 12 bytes in page
	IX_PreparePageDetailsSlot(IX_pageDetailstuple,NumOfRecs+1, RecOffset+recLen , freeSpace-recLen-12);
	offset = PF_PAGE_SIZE-12;
	memcpy((char*)IX_buffer+offset,IX_pageDetailstuple,12);
	free(IX_pageDetailstuple);
	return;
}



void IX_IndexHandle::IX_CalculateNumRecsOffsetFreeSpaceInPage(void* IX_buffer, unsigned* NumOfRecs, unsigned* RecOffset, unsigned* freeSpace)
{
	unsigned int offset = PF_PAGE_SIZE-12;

	memcpy(NumOfRecs,(char*)IX_buffer+offset,sizeof(int));
	offset += sizeof(int);

	memcpy(RecOffset,(char*)IX_buffer+offset,sizeof(int));
	offset += sizeof(int);

	memcpy(freeSpace,(char*)IX_buffer+offset,sizeof(int));
	return;
}

RC IX_IndexHandle::IX_CreateNode(IX_PageType pageType,PageNum &pagenum)
{

	unsigned int leftLink = 0,rightLink = 0,offset = 0;
	void *IX_tuple = malloc(16);
	vector <LeafPage_TypeIntAttr> leafNodeVector,leafNodeVector1;
	if(!this->pf_FileHandle.isOpen())
	{
		if(!this->pf_FileHandle.AppendNewPage())
		{
			pagenum = this->pf_FileHandle.GetNumberOfPages()-1;
			memcpy((char*)IX_tuple+offset,&pageType,sizeof(int));
			offset += sizeof(int);
			if(pageType == IndexPage)
			{
				memcpy((char*)IX_tuple+offset,&leftLink,sizeof(int));
				offset += sizeof(int);
			}
			if(pageType == LeafPage || pageType == OverflowPage)
			{
				memcpy((char*)IX_tuple+offset,&leftLink,sizeof(int));
				offset += sizeof(int);
				memcpy((char*)IX_tuple+offset,&rightLink,sizeof(int));
				offset += sizeof(int);
			}
			if(this->pf_FileHandle.WriteData(pagenum,0,offset,IX_tuple))
				return -1;
			this->IX_PreparePageDetailsSlot(IX_tuple,0,offset,PF_PAGE_SIZE - 12 - offset);

			if(this->pf_FileHandle.WriteData(pagenum,PF_PAGE_SIZE - 12,12,IX_tuple))
				return -1;
			return 0;
		}
		return -1;
	}
	return -1;
}

void IX_IndexHandle::IX_PreparePageTypeDetailsSlot(void* IX_tuple,IX_PageType IX_pageType, unsigned leftPointerPageNum)
{
	unsigned IX_Value = IX_pageType;
	unsigned offset = 0;
	memcpy((char*)IX_tuple+offset,&IX_Value,sizeof(int));
	offset += sizeof(int);
	IX_Value = leftPointerPageNum;
	memcpy((char*)IX_tuple+offset,&IX_Value,sizeof(int));
	return;
}

void IX_IndexHandle::IX_PreparePageDetailsSlot(void* IX_tuple, unsigned NumOfRecs, unsigned RecOffset, unsigned freeSpace)
{
	unsigned int offset = 0;

	memcpy((char*)IX_tuple+offset,&NumOfRecs,sizeof(int));
	offset += sizeof(int);
	memcpy((char*)IX_tuple+offset,&RecOffset,sizeof(int));
	offset += sizeof(int);
	memcpy((char*)IX_tuple+offset,&freeSpace,sizeof(int));
	return;
}
//  RC IX_Create_IndexPage_TypeIntAttr_Vector(vector<void*>& voidkeyrids, vector<IndexPage_TypeIntAttr>& keyrids);

//CREATE 7 types of vectors 3-Index, 3-Leaf, 1-Overflow - total 7 methods - pass vector<void*>& keyrids, and the vector<>& structs
RC IX_IndexHandle::IX_Create_IndexPage_TypeIntAttr_Vector(vector<void*>& voidkeyrids, vector<IndexPage_TypeIntAttr>& keyrids)
{
	IndexPage_TypeIntAttr keyrid;
	unsigned i;
	unsigned size = keyrids.size();
	for(i = 0; i < size; i++)
		keyrids.pop_back();
	keyrids.clear();

	for(unsigned i = 0; i < voidkeyrids.size(); i++)
	{
		memcpy(&keyrid.key,(char*)voidkeyrids[i], sizeof(int));
		memcpy(&keyrid.pageid,(char*)voidkeyrids[i]+sizeof(int), sizeof(int));
		keyrids.push_back(keyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_IndexPage_TypeRealAttr_Vector(vector<void*>& voidkeyrids, vector<IndexPage_TypeRealAttr>& keyrids)
{
	IndexPage_TypeRealAttr keyrid;
	unsigned i;
	unsigned size = keyrids.size();
	for(i = 0; i < size; i++)
		keyrids.pop_back();
	keyrids.clear();
	for(unsigned i = 0; i < voidkeyrids.size(); i++)
	{
		memcpy(&keyrid.key,(char*)voidkeyrids[i], sizeof(int));
		memcpy(&keyrid.pageid,(char*)voidkeyrids[i]+sizeof(int), sizeof(int));
		keyrids.push_back(keyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_IndexPage_TypeVarcharAttr_Vector(vector<void*>& voidkeyrids, vector<IndexPage_TypeVarcharAttr>& keyrids)
{
	IndexPage_TypeVarcharAttr keyrid;
	unsigned i;
	unsigned size = keyrids.size();
	for(i = 0; i < size; i++)
		keyrids.pop_back();
	keyrids.clear();

	for(unsigned i = 0; i < voidkeyrids.size(); i++)
	{
		memcpy(&keyrid.key,(char*)voidkeyrids[i], sizeof(int));
		memcpy(&keyrid.pageid,(char*)voidkeyrids[i]+sizeof(int), sizeof(int));
		keyrids.push_back(keyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_LeafPage_TypeIntAttr_Vector(vector<void*>& voidkeyrids, vector<LeafPage_TypeIntAttr>& keyrids)
{
	LeafPage_TypeIntAttr keyrid;
	unsigned i;
	unsigned size = keyrids.size();
	for(i = 0; i < size; i++)
		keyrids.pop_back();
	keyrids.clear();

	for(unsigned i = 0; i < voidkeyrids.size(); i++)
	{
		memcpy(&keyrid.key,(char*)voidkeyrids[i], sizeof(int));
		memcpy(&keyrid.hasDuplicate,(char*)voidkeyrids[i]+sizeof(int), sizeof(int));
		memcpy(&keyrid.rid.pageNum,(char*)voidkeyrids[i]+2*sizeof(int), sizeof(int));
		memcpy(&keyrid.rid.slotNum,(char*)voidkeyrids[i]+3*sizeof(int), sizeof(int));
		keyrids.push_back(keyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_LeafPage_TypeRealAttr_Vector(vector<void*>& voidkeyrids, vector<LeafPage_TypeRealAttr>& keyrids)
{
	LeafPage_TypeRealAttr keyrid;
	unsigned i;
	unsigned size = keyrids.size();
	for(i = 0; i < size; i++)
		keyrids.pop_back();
	keyrids.clear();
	for(unsigned i = 0; i < voidkeyrids.size(); i++)
	{
		memcpy(&keyrid.key,(char*)voidkeyrids[i], sizeof(int));
		memcpy(&keyrid.hasDuplicate,(char*)voidkeyrids[i]+sizeof(int), sizeof(int));
		memcpy(&keyrid.rid.pageNum,(char*)voidkeyrids[i]+2*sizeof(int), sizeof(int));
		memcpy(&keyrid.rid.slotNum,(char*)voidkeyrids[i]+3*sizeof(int), sizeof(int));
		keyrids.push_back(keyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_LeafPage_TypeVarcharAttr_Vector(vector<void*>& voidkeyrids, vector<LeafPage_TypeVarcharAttr>& keyrids)
{
	LeafPage_TypeVarcharAttr keyrid;
	unsigned i;
	unsigned size = keyrids.size();
	for(i = 0; i < size; i++)
		keyrids.pop_back();
	keyrids.clear();
	for(unsigned i = 0; i < voidkeyrids.size(); i++)
	{
		memcpy(&keyrid.key,(char*)voidkeyrids[i], sizeof(int));
		memcpy(&keyrid.hasDuplicate,(char*)voidkeyrids[i]+sizeof(int), sizeof(int));
		memcpy(&keyrid.rid.pageNum,(char*)voidkeyrids[i]+2*sizeof(int), sizeof(int));
		memcpy(&keyrid.rid.slotNum,(char*)voidkeyrids[i]+3*sizeof(int), sizeof(int));
		keyrids.push_back(keyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_OverflowPage_RIDs_Vector(vector<void*>& voidkeyrids, vector<RID>& keyrids)
{
	RID keyrid;
	unsigned i;
	unsigned size = keyrids.size();
	for(i = 0; i < size; i++)
		keyrids.pop_back();
	keyrids.clear();
	for(unsigned i = 0; i < voidkeyrids.size(); i++)
	{
		memcpy(&keyrid.pageNum,(char*)voidkeyrids[i], sizeof(int));
		memcpy(&keyrid.slotNum,(char*)voidkeyrids[i]+sizeof(int), sizeof(int));
		keyrids.push_back(keyrid);
	}
	return 0;
}


RC IX_IndexHandle::IX_Create_IndexPage_TypeIntAttr_VoidVector(vector<void*>& voidkeyrids, vector<IndexPage_TypeIntAttr>& keyrids)
{
	void* voidkeyrid;
	unsigned offset;
	unsigned i;
	for(i = 0; i < voidkeyrids.size(); i++)
		free(voidkeyrids[i]);
	voidkeyrids.clear();
	for(unsigned i = 0; i < keyrids.size(); i++)
	{
		voidkeyrid = malloc(8);
		offset = 0;
		memcpy((char*)voidkeyrid+offset,&keyrids[i].key,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].pageid,sizeof(int));
		voidkeyrids.push_back(voidkeyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_IndexPage_TypeRealAttr_VoidVector(vector<void*>& voidkeyrids, vector<IndexPage_TypeRealAttr>& keyrids)
{
	void* voidkeyrid;
	unsigned offset;
	unsigned i;
	for(i = 0; i < voidkeyrids.size(); i++)
		free(voidkeyrids[i]);
	voidkeyrids.clear();
	for(unsigned i = 0; i < keyrids.size(); i++)
	{
		voidkeyrid = malloc(8);
		offset = 0;
		memcpy((char*)voidkeyrid+offset,&keyrids[i].key,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].pageid,sizeof(int));
		voidkeyrids.push_back(voidkeyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_IndexPage_TypeVarcharAttr_VoidVector(vector<void*>& voidkeyrids, vector<IndexPage_TypeVarcharAttr>& keyrids)
{
	void* voidkeyrid;
	unsigned offset;
	unsigned i;
	for(i = 0; i < voidkeyrids.size(); i++)
		free(voidkeyrids[i]);
	voidkeyrids.clear();
	for(unsigned i = 0; i < keyrids.size(); i++)
	{
		voidkeyrid = malloc(8);
		offset = 0;
		memcpy((char*)voidkeyrid+offset,keyrids[i].key,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].pageid,sizeof(int));
		voidkeyrids.push_back(voidkeyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_LeafPage_TypeIntAttr_VoidVector(vector<void*>& voidkeyrids, vector<LeafPage_TypeIntAttr>& keyrids)
{
	void* voidkeyrid;
	unsigned offset;
	unsigned i;
	for(i = 0; i < voidkeyrids.size(); i++)
		free(voidkeyrids[i]);
	voidkeyrids.clear();

	for(unsigned i = 0; i < keyrids.size(); i++)
	{
		voidkeyrid = malloc(16);
		offset = 0;
		memcpy((char*)voidkeyrid+offset,&keyrids[i].key,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].hasDuplicate,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].rid.pageNum,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].rid.slotNum,sizeof(int));
		voidkeyrids.push_back(voidkeyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_LeafPage_TypeRealAttr_VoidVector(vector<void*>& voidkeyrids, vector<LeafPage_TypeRealAttr>& keyrids)
{
	void* voidkeyrid;
	unsigned offset;
	unsigned i;
	for(i = 0; i < voidkeyrids.size(); i++)
		free(voidkeyrids[i]);
	voidkeyrids.clear();
	for(unsigned i = 0; i < keyrids.size(); i++)
	{
		voidkeyrid = malloc(16);
		offset = 0;
		memcpy((char*)voidkeyrid+offset,&keyrids[i].key,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].hasDuplicate,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].rid.pageNum,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].rid.slotNum,sizeof(int));
		voidkeyrids.push_back(voidkeyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_LeafPage_TypeVarcharAttr_VoidVector(vector<void*>& voidkeyrids, vector<LeafPage_TypeVarcharAttr>& keyrids)
{
	void* voidkeyrid;
	unsigned offset;
	unsigned i;
	for(i = 0; i < voidkeyrids.size(); i++)
		free(voidkeyrids[i]);
	voidkeyrids.clear();
	for(unsigned i = 0; i < keyrids.size(); i++)
	{
		voidkeyrid = malloc(16);
		offset = 0;
		memcpy((char*)voidkeyrid+offset,keyrids[i].key,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].hasDuplicate,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].rid.pageNum,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].rid.slotNum,sizeof(int));
		voidkeyrids.push_back(voidkeyrid);
	}
	return 0;
}

RC IX_IndexHandle::IX_Create_OverflowPage_RIDs_VoidVector(vector<void*>& voidkeyrids, vector<RID>& keyrids)
{
	void* voidkeyrid;
	unsigned offset;
	unsigned i;
	for(i = 0; i < voidkeyrids.size(); i++)
		free(voidkeyrids[i]);
	voidkeyrids.clear();

	for(unsigned i = 0; i < keyrids.size(); i++)
	{
		voidkeyrid = malloc(8);
		offset = 0;
		memcpy((char*)voidkeyrid+offset,&keyrids[i].pageNum,sizeof(int));
		offset += sizeof(int);
		memcpy((char*)voidkeyrid+offset,&keyrids[i].slotNum,sizeof(int));
		voidkeyrids.push_back(voidkeyrid);
	}
	return 0;
}
RC IX_IndexHandle::IX_CreateVoidVectorFromPage(unsigned pageNum
							,unsigned *leftPointerPageNum,unsigned *rightPointerPageNum
							,IX_PageType *ix_pageType, AttrType attrType, vector<void*>& keyrids)
{

	//Open file and fetch the page
	//Fill leftPointerPageNum and rightPointerPageNum based on the pageType
	//Fetch #of records
	//Loop thru the records and keep appending data to the vector - 8- index and overflow, 16- leaf(key+hasDup+RID)
	RC rc;
	void* IX_buffer = malloc(PF_PAGE_SIZE);
	memset(IX_buffer,0x0,PF_PAGE_SIZE);
	void* IX_tuple;
	unsigned numOfRecords,offset;
	unsigned recLen;
	unsigned i;
	unsigned size = keyrids.size();
	for(i = 0; i < size; i++)
		keyrids.pop_back();
	keyrids.clear();

	rc = this->pf_FileHandle.ReadPage(pageNum,IX_buffer);
	if(rc != success)
	{
		free(IX_buffer);
		return rc;
    }

	memcpy(ix_pageType,(char*)IX_buffer, sizeof(int));
	memcpy(leftPointerPageNum, (char*)IX_buffer+sizeof(int), sizeof(int));
	if(*ix_pageType == LeafPage)
	{
		memcpy(rightPointerPageNum, (char*)IX_buffer+2*sizeof(int), sizeof(int));
		recLen = 16;
		offset = 12;
	}
	else if(*ix_pageType == OverflowPage)
	{
		memcpy(rightPointerPageNum, (char*)IX_buffer+2*sizeof(int), sizeof(int));
		recLen = 8;
		offset = 12;
	}

	else
	{
		recLen = 8;
		offset = 8;
	}
	memcpy(&numOfRecords, (char*)IX_buffer+PF_PAGE_SIZE-12, sizeof(int));

	for(unsigned i = 0; i < numOfRecords; i++)
	{
		IX_tuple = malloc(recLen);
		memcpy(IX_tuple,(char*)IX_buffer+offset,recLen);
		keyrids.push_back(IX_tuple);
		offset += recLen;
	}

	free(IX_buffer);
	return 0;
}
