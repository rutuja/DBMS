#include "rm.h"

const char* TableCatalog = "tableCatalog";
const char* ColumnCatalog = "columnCatalog";
const int success = 0;

RM* RM::_rm = 0;
PF_Manager *pf;
RC rc;

#define PF_PAGE_SIZE 4096

#define NUM_OF_FIELDS_IN_TBL_CATALOG 3
#define NUM_OF_FIELDS_IN_COL_CATALOG 6
#define MAX_FIELD_SIZE_TBL_CATALOG 20
#define TABLE_CATALOG_TABLE_ID 1
#define COLUMN_CATALOG_TABLE_ID 2

RM* RM::Instance()
{
    if(!_rm)
    {
        _rm = new RM();
        pf = PF_Manager::Instance();
        _rm->createCatalog();
    }
    return _rm;
}

RM::RM()
{

}

RM::~RM()
{

}

RC RM::createCatalog()
{
	rc = -1;
	if(!pf->CreateFile(TableCatalog)&&!pf->CreateFile(ColumnCatalog))
		rc = this->intializeCatalog();
	return rc;
}

void RM::prepareTableCatalogTuple(const int tableID, const int tableNameLength,const char *tableName, const int fileNameLength,const char *fileName, void *tuple, int *tuple_size)
{
    int offset = 0;

    memcpy ((char *)tuple+offset, &tableID,sizeof(int));
    offset += sizeof(int);

    memcpy((char *)tuple + offset, &(tableNameLength), sizeof(int));
    offset += sizeof(int);

    memcpy((char *)tuple + offset, tableName, tableNameLength);
    offset += tableNameLength;

    memcpy((char *)tuple + offset, &(fileNameLength), sizeof(int));
    offset += sizeof(int);

    memcpy((char *)tuple + offset, fileName, fileNameLength);
    offset += fileNameLength;

    *tuple_size = offset;

}

void RM::printTableCatalogTuple(const void *tuple, const int tuple_size)
{
	int offset = 0;

	int tableID = 0,tableNameLength =0,fileNameLength =0;
	memcpy(&tableID, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("\n %d  ", tableID);

	memcpy(&tableNameLength, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("%d  ", tableNameLength);


	char *tableName = (char *)malloc(100);
	memcpy(tableName, (char *)tuple+offset, tableNameLength);
	tableName[tableNameLength] = '\0';
	offset += tableNameLength;
	printf("%s  ", tableName);

	memcpy(&fileNameLength, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("%d  ", fileNameLength);


	char *fileName = (char *)malloc(100);
	memcpy(fileName, (char *)tuple+offset, fileNameLength);
	fileName[fileNameLength] = '\0';
	offset += fileNameLength;
	printf("%s  ", fileName);

}

void RM::prepareColumnCatalogTuple(const int tableID, const int columnNameLength,const char *columnName, const int columnType,const int columnLength,const int columnPosition,const int isDeleted, void *tuple, int *tuple_size)
{
	int offset = 0;

	memcpy ((char *)tuple+offset, &tableID,sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, &(columnNameLength), sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, columnName, columnNameLength);
	offset += columnNameLength;

	memcpy((char *)tuple + offset, &(columnType), sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, &(columnLength), sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, &(columnPosition), sizeof(int));
	offset += sizeof(int);

	memcpy((char *)tuple + offset, &(isDeleted), sizeof(int));
	offset += sizeof(int);

	*tuple_size = offset;
}

void RM::printColumnCatalogTuple(const void *tuple, const int tuple_size)
{
	int offset = 0;

	int tableID = 0,columnNameLength =0,columnType=0,columnPosition=0,columnLength=0,isDeleted=0;

	memcpy(&tableID, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("\n %d  ", tableID);

	memcpy(&columnNameLength, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("%d  ", columnNameLength);


	char *columnName = (char *)malloc(100);
	memcpy(columnName, (char *)tuple+offset, columnNameLength);
	columnName[columnNameLength] = '\0';
	offset += columnNameLength;
	printf("%s  ", columnName);

	memcpy(&columnType, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("%d  ", columnType);

	memcpy(&columnLength, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("%d  ", columnLength);

	memcpy(&columnPosition, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("%d  ", columnPosition);

	memcpy(&isDeleted, (char *)tuple+offset, sizeof(int));
	offset += sizeof(int);
	printf("%d  ", isDeleted);

	free(columnName);

}

RC RM::intializeCatalog()
{
	PF_FileHandle fileHandle1, fileHandle2;
	RID rid;
	void *tuple = malloc(100),*finalTuple;
	int tupleSize = 0;
	unsigned tableid[2] = {1,2};
	unsigned length[2] = {12,13};
	string tablename[2] = {"tableCatalog","columnCatalog"};
	for(int i=0;i<2;i++)
	{
		this->prepareTableCatalogTuple(tableid[i],length[i],tablename[i].c_str(),length[i],tablename[i].c_str(),tuple,&tupleSize);
		finalTuple = malloc(tupleSize);
		memcpy((char *)finalTuple, (char *)tuple,tupleSize);
		rc = this->insertTuple(TableCatalog,finalTuple,rid);
		free(finalTuple);

	}

	unsigned tableid1[9] = {1,1,1,2,2,2,2,2,2};
	unsigned length1[9] = {8,10,9,8,11,11,13,15,8};
	string columnName[9] = {"table-id","table-name","file-name","table-id","column-name",
							"column-type","column-length","column-position","isDeleted"};
	unsigned columntype[9] = {0,2,2,0,2,0,0,0,0};
	unsigned columnLength[9] = {4,20,20,4,20,4,4,4,4};
	unsigned columnPotition[9] = {1,2,3,1,2,3,4,5,6};
	unsigned isDeleted[9] = {0,0,0,0,0,0,0,0,0};

	for(int i=0;i<9;i++)
	{
		this->prepareColumnCatalogTuple(tableid1[i],length1[i],columnName[i].c_str(),columntype[i],columnLength[i],
										columnPotition[i],isDeleted[i],tuple,&tupleSize);
		finalTuple = malloc(tupleSize);
		memcpy((char *)finalTuple, (char *)tuple,tupleSize);
		rc = this->insertTuple(ColumnCatalog,finalTuple,rid);
		free(finalTuple);

	}
	free(tuple);
	return 0;

}
unsigned RM::createTableID()
{
	PF_FileHandle fileHandle;
	fileHandle.OpenFile(TableCatalog);
	void *buffer = malloc(PF_PAGE_SIZE);
	unsigned int pageNumInSlot;
	unsigned int slotNumInSlot = 0;
    unsigned int maxNumOfPages, pageNumber;
    unsigned int tableID = 0;
    unsigned int isBreak = 0;
    maxNumOfPages = fileHandle.GetNumberOfPages();
    for(pageNumber = maxNumOfPages-1; pageNumber >=0; pageNumber--)
    {
		rc = fileHandle.ReadPage(pageNumber, buffer);
		if(rc != success)
			return rc;
		unsigned int numRecsInPage;
		memcpy(&numRecsInPage,(char *)buffer+PF_PAGE_SIZE-12 ,4);
		for(unsigned int i = numRecsInPage-1; i >= 0; i--)
		{
			memcpy(&pageNumInSlot, (char*)buffer+(PF_PAGE_SIZE-12-(i+1)*12), sizeof(int));
			if(pageNumInSlot != 9999)
			{
				memcpy(&slotNumInSlot,(char*)buffer+(PF_PAGE_SIZE-12-(i+1)*12)+4,sizeof(int));
				isBreak = 1;
				break;
			}
		}
		if(isBreak ==1)
			break;
    }
    tableID = (maxNumOfPages-1)*100+slotNumInSlot+2;
	fileHandle.CloseFile();
	free(buffer);
	if(isBreak ==1)
		return tableID;
	else
		return 0;
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
	RID rid;
	void * tuple = malloc(100);
	int tupleSize = 0,tableid = this->createTableID();
	Attribute attr;
	rc = pf->CreateFile(tableName.c_str());
	if(rc==0)
	{
		this->prepareTableCatalogTuple(tableid,tableName.length(),tableName.c_str(),tableName.length(),tableName.c_str(),tuple,&tupleSize);
		void *finalTuple = malloc(tupleSize);
		memcpy((char *)finalTuple, (char *)tuple,tupleSize);
		rc = this->insertTuple(TableCatalog,finalTuple,rid);
		free(finalTuple);

		for(unsigned i=0;i<attrs.size();i++)
		{
			attr = attrs[i];
			this->prepareColumnCatalogTuple(tableid,attr.name.length(),attr.name.c_str(),attr.type,attr.length,i+1,0,tuple,&tupleSize);
			finalTuple = malloc(tupleSize);
			memcpy((char *)finalTuple, (char *)tuple,tupleSize);
			rc = this->insertTuple(ColumnCatalog,finalTuple,rid);
			free(finalTuple);
		}
		free(tuple);
		return 0;
	}
	else
		return -1;
}

unsigned int RM::getTupleSize(const string tableName, const void* data)
{
	if ((data != 0) && (tableName == TableCatalog))
	{
		int attr_len;
		int tuple_size = 0;

		tuple_size  = 4; //for id

		memcpy(&attr_len,(char*)data+tuple_size,sizeof(int));
		tuple_size += 4; //for attr_name_len field
		tuple_size += attr_len; //for attr_name string

		memcpy(&attr_len,(char*)data+tuple_size,sizeof(int));
		tuple_size += 4; //for filename_len field
		tuple_size += attr_len; //for table_name string

		return tuple_size;
	}
	else if ((data != 0) && (tableName == ColumnCatalog))
	{
		int attr_len;
		int tuple_size = 0;

		tuple_size  = 4; //for id

		memcpy(&attr_len,(char*)data+tuple_size,sizeof(int));
		tuple_size += 4; //for table_name_len field
		tuple_size += attr_len; //for table_name string

		tuple_size += 4; //for type
		tuple_size += 4; //for length
		tuple_size += 4; //col. pos
		tuple_size += 4; //is deleted

		return tuple_size;
	}
	else if (data != 0)
	{
		int tuple_size = 0;
		vector<RID> ridsInColumnCatalogDummy;
		RID ridInTableCatalogDummy;
		vector<Attribute_Internal> attrs;
		findDetailsOfFieldsInATable(tableName, attrs, ridInTableCatalogDummy ,ridsInColumnCatalogDummy);

		for(unsigned i =0; i < attrs.size(); i++)
		{
			if(!attrs[i].isDeleted)
			{
				if(attrs[i].type==0)
				{
					tuple_size+=4;
				}

				if(attrs[i].type==1)
				{
					tuple_size+=4;
				}
				if(attrs[i].type==2)
				{
					unsigned length;
					
					memcpy (&length,(char*)data+tuple_size,sizeof(int));
					tuple_size+=sizeof(int);
					tuple_size+=length;
				}
			}
		}
		return tuple_size;
	}

	return 0;
}

RC RM::insertTuple(const string tableName, const void *tuple, RID &rid)
{

	PF_FileHandle fileHandle;
	OFFSET offset;
	if(!pf->OpenFile(tableName.c_str(),fileHandle))
	{
		if(fileHandle.GetNumberOfPages()==0)
		{
			fileHandle.AppendNewPage();

            rid.pageNum = 0;
			offset  = this->insertRID(fileHandle,rid);

			rc = fileHandle.WriteData(0,offset,getTupleSize(tableName, (void *)tuple), tuple);

			//update free space
			rc = this->updateByte(fileHandle,getTupleSize(tableName,(void *)tuple), 0, PF_PAGE_SIZE-4,false);
			//update offset
			rc = this->updateByte(fileHandle,getTupleSize(tableName,(void *)tuple), 0, PF_PAGE_SIZE-8,true);
			return rc;
		}
		else if (fileHandle.findFreeSpace(getTupleSize(tableName,(void *)tuple),rid.pageNum)== -1)
		{
			fileHandle.AppendNewPage();

			rid.pageNum = fileHandle.GetNumberOfPages()-1;
			//calculate free space and send page number

			offset  = this->insertRID(fileHandle,rid);
			rc = fileHandle.WriteData(rid.pageNum,offset,getTupleSize(tableName,(void *)tuple), tuple);

			//update free space
			rc = this->updateByte(fileHandle,getTupleSize(tableName,(void *)tuple), rid.pageNum, PF_PAGE_SIZE-4,false);
			//update offset
			rc = this->updateByte(fileHandle,getTupleSize(tableName,(void *)tuple), rid.pageNum, PF_PAGE_SIZE-8,true);
			return rc;

		}
		else
		{
			rc = fileHandle.findFreeSpace(getTupleSize(tableName,(void *)tuple),rid.pageNum);
			offset = this->insertRID(fileHandle,rid);
			rc = fileHandle.WriteData(rid.pageNum,offset,getTupleSize(tableName,(void *)tuple), tuple);
			//update free space
			rc = this->updateByte(fileHandle,getTupleSize(tableName,(void *)tuple), rid.pageNum, PF_PAGE_SIZE-4,false);
			//update offset
			rc = this->updateByte(fileHandle,getTupleSize(tableName,(void *)tuple), rid.pageNum, PF_PAGE_SIZE-8,true);
			return rc;
		}
		fileHandle.CloseFile();
		return 0;
	}
	return -1;
}

unsigned RM::insertRID(PF_FileHandle & fileHandle,RID &rid)
{
	//Reading number of records in a page
	const void *data_noOfSlots = malloc(4),*data_freeSpace = malloc(4),*data3 = malloc(12),*data_offset = malloc(4);
	unsigned noOfSlots,freeSpace;
	OFFSET offset;
	//read free space
	rc = fileHandle.ReadData(rid.pageNum,PF_PAGE_SIZE-4,data_freeSpace,4);
	//read offset for new record
	rc = fileHandle.ReadData(rid.pageNum,PF_PAGE_SIZE-8, data_offset,4);
	//read number of records
	rc = fileHandle.ReadData(rid.pageNum,PF_PAGE_SIZE-12, data_noOfSlots,4);

	memcpy(&noOfSlots, (char *)data_noOfSlots, sizeof(int));
	memcpy(&freeSpace, (char *)data_freeSpace, sizeof(int));
	memcpy(&offset, (char *)data_offset, sizeof(int));

	//update slot number
	rid.slotNum = noOfSlots;

	//create actual RID to insert in page
	//create slot number

	memcpy((char*)data3,&rid.pageNum,4);
	memcpy((char*)data3+4,&rid.slotNum,4);
	memcpy((char*)data3+8,&offset,4);

	//insert RID
	rc = fileHandle.WriteData(rid.pageNum,PF_PAGE_SIZE-12-((++noOfSlots)*12),12, data3);

	freeSpace = freeSpace-12;
	memcpy((char*)data_noOfSlots,&noOfSlots,4);
	memcpy((char*)data_freeSpace,&freeSpace,4);


	rc = fileHandle.WriteData(rid.pageNum,PF_PAGE_SIZE-4,4,data_freeSpace);
	rc = fileHandle.WriteData(rid.pageNum,PF_PAGE_SIZE-12,4,data_noOfSlots);

	free((void*)data_noOfSlots);
	free((void*)data_freeSpace);
	free((void*)data_offset);
	free((void*)data3);

	return offset;
}

RC RM::updateByte(PF_FileHandle & fileHandle, unsigned size, PageNum pagenum,OFFSET offset,bool add)
{
	const void *data1 = malloc(4);
	int noOfSlots =0 ;
	//update free space
	rc = fileHandle.ReadData(pagenum,offset,data1,4);
	memcpy(&noOfSlots, (char *)data1, sizeof(int));

	if(add==true)
		noOfSlots = noOfSlots + size;
	else
		noOfSlots = noOfSlots - size;

	memcpy((char*)data1,&noOfSlots,4);

	rc = fileHandle.WriteData(pagenum,offset,4,data1);
	free((void *)data1);
	return 0;
}

bool RM::compareAttribute(const void *attrValue,const CompOp compOp,const void *givenValue,AttrType attrType,unsigned attrValuelength)
{
	if(attrType == TypeInt)
	{
		unsigned attrvalue = 0,givenvalue = 0;
		memcpy(&attrvalue, (char *)attrValue, sizeof(int));
		memcpy(&givenvalue, (char *)givenValue, sizeof(int));

		if(compOp==EQ_OP)
		{
			if(attrvalue==givenvalue)
				return true;
		}
		else if(compOp==LT_OP)
		{
			if(attrvalue<givenvalue)
				return true;
		}
		else if(compOp==GT_OP)
		{
			if(attrvalue>givenvalue)
				return true;
		}
		else if(compOp==LE_OP)
		{
			if(attrvalue<=givenvalue)
				return true;
		}
		else if(compOp==GE_OP)
		{
			if(attrvalue>=givenvalue)
				return true;
		}
		else if(compOp==NE_OP)
		{
			if(attrvalue!=givenvalue)
				return true;
		}
		else if(compOp==NO_OP)
			return true;
		else
			return false;
	}

	if(attrType == TypeVarChar)
	{
		unsigned givenValuelength;
		memcpy (&givenValuelength,(char *)givenValue,sizeof(int));

		char *name = (char *)malloc(100);
	    memcpy(name, (char *)givenValue+sizeof(int), givenValuelength);

	    name[givenValuelength] = '\0';
	    string s2 = name;
	    free(name);

		char * name1 = (char *)malloc(100);
	    memcpy(name1, (char *)attrValue, attrValuelength);
	    name1[attrValuelength] = '\0';
	    string s1 = name1;
	    free(name1);
	    if(givenValuelength == attrValuelength)
		{

			if(compOp==EQ_OP)
			{
				if(s1.compare(s2)==0)
					return true;
			}
			else if(compOp==LT_OP)
			{
				if(s1.compare(s2)<0)
					return true;
			}
			else if(compOp==GT_OP)
			{
				if(s1.compare(s2)>0)
					return true;
			}
			else if(compOp==LE_OP)
			{
				if(s1.compare(s2)<=0)
					return true;
			}
			else if(compOp==GE_OP)
			{
				if(s1.compare(s2)>=0)
					return true;
			}
			else if(compOp==NE_OP)
			{
				if(s1.compare(s2)!=0)
					return true;
			}
			else if(compOp==NO_OP)
				return true;
			else
				return false;
		}
		else
			return false;
	}
	if(attrType == TypeReal)
		{
			float attrvalue = 0,givenvalue = 0;
			memcpy(&attrvalue, (char *)attrValue, sizeof(int));
			memcpy(&givenvalue, (char *)givenValue, sizeof(int));

			if(compOp==EQ_OP)
			{
				if(attrvalue==givenvalue)
					return true;
			}
			else if(compOp==LT_OP)
			{
				if(attrvalue<givenvalue)
					return true;
			}
			else if(compOp==GT_OP)
			{
				if(attrvalue>givenvalue)
					return true;
			}
			else if(compOp==LE_OP)
			{
				if(attrvalue<=givenvalue)
					return true;
			}
			else if(compOp==GE_OP)
			{
				if(attrvalue>=givenvalue)
					return true;
			}
			else if(compOp==NE_OP)
			{
				if(attrvalue!=givenvalue)
					return true;
			}
			else if(compOp==NO_OP)
				return true;
			else
				return false;
		}
	return false;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	scaniteratortuple sit;
	if(scannedData.size()>=1)
	{
		sit = this->scannedData[scannedData.size()-1];
		rid.pageNum = sit.rid.pageNum;
		rid.slotNum = sit.rid.slotNum;
		memcpy((char *)data,(char *)sit.data,sit.dataLength);
		free(sit.data);
		this->scannedData.pop_back();
		return 0;
	}
	else
		return RM_EOF;
}


RC RM_ScanIterator::insertNextTuple(RID &rid, void *data,unsigned length)
{
	scaniteratortuple sit;
	sit.data = malloc(length);
	memcpy((char *)sit.data,(char *)data,length);
	sit.rid.pageNum = rid.pageNum;
	sit.rid.slotNum = rid.slotNum;
	sit.dataLength = length;
	this->scannedData.push_back(sit);
	return 0;
}

RC RM::createProjectedTuple(vector <void *> splittedTuple,const vector<string> &attributeNames, vector<Attribute_Internal> tableAttrs, void *projectedTuple,unsigned &projectedTupleSize)
{
	OFFSET offset = 0 ;
	int length;
	for(unsigned i = 0,k=0;i<tableAttrs.size();i++,k++)
	{
		if(tableAttrs[i].type==TypeVarChar)
			k++;
		for(unsigned j = 0;j<attributeNames.size();j++)
		{
			if(tableAttrs[i].name.compare(attributeNames[j])==0)
			{
				if(tableAttrs[i].type==TypeVarChar)
				{
					memcpy ((char *)projectedTuple+offset,(char *)splittedTuple[k-1],4);
					offset=offset+4;
					memcpy (&length,(char *)splittedTuple[k-1],4);

					memcpy ((char *)projectedTuple+offset,(char *)splittedTuple[k],length);
					offset=offset+length;
				}
				else
				{
					memcpy ((char *)projectedTuple+offset,(char *)splittedTuple[k],4);
					offset+=4;
				}
			}
		}
	}
	projectedTupleSize = offset;
	return 0;
}
RC RM::createProjectedTupleWithoutVarchar(vector <void *> splittedTuple,const vector<string> &attributeNames, vector<Attribute> tableAttrs, void *projectedTuple,unsigned &projectedTupleSize)
{
	OFFSET offset = 0 ;
	for(unsigned i = 0;i<attributeNames.size();i++)
	{
		for(unsigned j = 0;j<tableAttrs.size();j++)
		{
			if(tableAttrs[j].name.compare(attributeNames[i])==0)
			{
				memcpy ((char *)projectedTuple+offset,(char *)splittedTuple[j],4);
				offset+=4;
			}
		}
	}
	projectedTupleSize = offset;
	return 0;
}
RC RM::splitTuple(const void * tuple, vector<Attribute_Internal> tableAttrs,vector <void * > &splittedTuple)
{
	OFFSET offset=0;
	int k=0;
	for(unsigned i =0;i<tableAttrs.size();i++)
	{
		if(!tableAttrs[i].isDeleted)
		{
			if(tableAttrs[i].type==TypeInt)
			{
				k++;
				splittedTuple.push_back((void *)tuple+offset);
				offset+=4;
			}
			if(tableAttrs[i].type==TypeReal)
			{
				k++;
				splittedTuple.push_back((void *)tuple+offset);
				offset+=4;
			}
			if(tableAttrs[i].type==TypeVarChar)
			{
				k = k+2;
				unsigned length;
				memcpy (&length,(char *)tuple+offset,sizeof(int));
				splittedTuple.push_back((void *)tuple+offset);
				offset+=sizeof(int);
				splittedTuple.push_back((void *)tuple+offset);
				offset= offset + length;
			}
		}
	}
	return 0;
}

RC RM::QE_splitTuple(const void * tuple, vector<Attribute> tableAttrs,vector <void * > &splittedTuple)
{
	OFFSET offset=0;
	for(unsigned i =0;i<tableAttrs.size();i++)
	{
			if(tableAttrs[i].type==TypeInt)
			{
				splittedTuple.push_back((char *)tuple+offset);
				offset+=4;
			}
			if(tableAttrs[i].type==TypeReal)
			{
				splittedTuple.push_back((char *)tuple+offset);
				offset+=4;
			}
			if(tableAttrs[i].type==TypeVarChar)
			{
				unsigned length;
				memcpy (&length,(char *)tuple+offset,sizeof(int));
				splittedTuple.push_back((char *)tuple+offset);
				offset += sizeof(int)+length;
			}
	}
	return 0;
}

RC RM::scan(const string tableName,const string conditionAttribute, const CompOp compOp,const void *value, const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator)
{
	PF_FileHandle fileHandle2;
	void * data_noOfSlots = malloc(4);
	void * recordID       = malloc(12);
	void * tuple          = malloc(200);
	void * attrValue      = 0;
	void *projectedTuple = malloc(200);
	
	RID rid;
	unsigned noOfSlots,pageNum,offset,projectedTupleSize;
	if(!pf->OpenFile(tableName.c_str(),fileHandle2))
	{
		vector<RID> ridsInColumnCatalogDummy;
		RID ridInTableCatalogDummy;
		vector<Attribute_Internal> Tableattrs;
		AttrType attrType;
		findDetailsOfFieldsInATable(tableName, Tableattrs, ridInTableCatalogDummy ,ridsInColumnCatalogDummy);
		for(unsigned i=0;i<fileHandle2.GetNumberOfPages();i++)
		{
			rc = fileHandle2.ReadData(i,PF_PAGE_SIZE-12, data_noOfSlots,4);
			memcpy(&noOfSlots, (char *)data_noOfSlots, sizeof(int));
			for(unsigned j=1;j<=noOfSlots;j++)
			{
				rc = fileHandle2.ReadData(i,PF_PAGE_SIZE-12-(j*12), recordID,12);
				memcpy(&pageNum, (char *)recordID, sizeof(int));
				memcpy(&offset, (char *)recordID+8, sizeof(int));

				rid.pageNum = i;
				rid.slotNum =j-1;
				rc = this->readTuple(tableName,rid,tuple);
				if(rc != -1)
				{
					vector <void *> splittedTuple;
					rc = this->splitTuple(tuple,Tableattrs,splittedTuple);

					if(conditionAttribute.compare("")==0)
					{
							rc = this->createProjectedTuple(splittedTuple,attributeNames,Tableattrs,projectedTuple,projectedTupleSize);
							void * ptuple = malloc(projectedTupleSize);
							memcpy((char *)ptuple, (char *)projectedTuple, projectedTupleSize);
							rm_ScanIterator.insertNextTuple(rid,ptuple,projectedTupleSize);
							
							free(ptuple);
							ptuple = 0;
					}
					else
					{
						unsigned length = 0;
						unsigned match_found = 0;
						
						for(unsigned int k = 0,l=0; k < Tableattrs.size(); k++,l++)
						{
							if(Tableattrs[k].isDeleted == 0)
							{
								if(Tableattrs[k].type==TypeVarChar)
									l++;
								if(Tableattrs[k].name.compare(conditionAttribute.c_str())==0)
								{
									match_found = 1;
									attrType = Tableattrs[k].type;
									if(Tableattrs[k].type==TypeVarChar)
									{
										memcpy (&length,(char *)splittedTuple[0],sizeof(int));
										attrValue = malloc(length);
										memcpy ((char *)attrValue,(char *)splittedTuple[l],length);
										break;

									}
									else
									{
										attrValue = malloc(4);
										memcpy ((char *)attrValue,(char *)splittedTuple[l],sizeof(int));
										break;
									}
								}
							}
						}

						if(match_found && compareAttribute(attrValue,compOp,value,attrType,length))
						{
							rc = this->createProjectedTuple(splittedTuple,attributeNames,Tableattrs,projectedTuple,projectedTupleSize);
							void * ptuple = malloc(projectedTupleSize);
							memcpy((char *)ptuple, (char *)projectedTuple, projectedTupleSize);
							rm_ScanIterator.insertNextTuple(rid,ptuple,projectedTupleSize);
							
							free(ptuple);
							ptuple = 0;
												
							free(attrValue);
							attrValue = 0;
						}
					}
				}
			}
		}
	}
	free(data_noOfSlots);
	free(recordID);
	free(tuple);
    free(projectedTuple);

	return 0;
}

RC RM::deleteTable(const string tableName)
{
	vector<Attribute_Internal> attrsThisTable;
	vector<RID> ridsInColumnCatalog;
	RID ridInTableCatalog;

	rc = pf->DestroyFile(tableName.c_str());
	if(rc != success)
		return rc;

	rc = findDetailsOfFieldsInATable(tableName, attrsThisTable, ridInTableCatalog ,ridsInColumnCatalog);
	if(rc != success)
		return rc;

	for(unsigned int i = 0; i < (unsigned int)ridsInColumnCatalog.size(); i++)
	{
		rc = deleteTuple(ColumnCatalog,ridsInColumnCatalog[i]);
		if(rc != success)
			return rc;
	}

	rc = deleteTuple(TableCatalog, ridInTableCatalog);
	return rc;
}

RC RM::deleteTuple(const string tableName, const RID &rid)
{
	PF_FileHandle fileHandle;
	PgNumOffset pgNumOffset;
	fileHandle.OpenFile(tableName.c_str());
	void *buffer = malloc(PF_PAGE_SIZE);
	RID rid_in_updated_page;

	fileHandle.ReadPage(rid.pageNum, buffer);
	rc = findRecordOffset(buffer ,rid, pgNumOffset);
	if(rc != success)
		return rc;

	if((pgNumOffset.PageNum != rid.pageNum) && (pgNumOffset.PageNum != 9999))
	{
		rid_in_updated_page.pageNum = pgNumOffset.PageNum;
		rid_in_updated_page.slotNum = pgNumOffset.SlotNum;
		rc = deleteTuple(tableName, rid_in_updated_page);
		if(rc != success)
			return rc;
	}
	else if(pgNumOffset.PageNum != 9999)//check for deleted record
	{
		rc = deleteRecord(buffer, pgNumOffset);
		if(rc != success)
			return rc;
		rc = fileHandle.WritePage(rid.pageNum, buffer);
		if(rc != success)
					return rc;
	}
	else
		return -1;

	fileHandle.CloseFile();
	free(buffer);
	return 0;
}

RC RM::deleteTuples(const string tableName)
{
	PF_FileHandle fileHandle;
	rc = pf->DeleteAllPages(tableName.c_str());
	if(rc != success)
		return rc;
	rc = fileHandle.OpenFile(tableName.c_str());
	if(rc != success)
		return rc;
	rc = fileHandle.CloseFile();
	return rc;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{
	attrs.clear();
	vector<RID> ridsInColumnCatalogDummy;
	RID ridInTableCatalogDummy;
	vector<Attribute_Internal> attrsDummy;
	attrsDummy.clear();
	Attribute a;
	rc = findDetailsOfFieldsInATable(tableName, attrsDummy, ridInTableCatalogDummy ,ridsInColumnCatalogDummy);
	if(rc != success)
		return rc;
	for(unsigned int i = 0; i < attrsDummy.size(); i++)
	{
		if(attrsDummy[i].isDeleted == 0)
		{
			a.name = attrsDummy[i].name;
			a.type = attrsDummy[i].type;
			a.length = attrsDummy[i].length;
			attrs.push_back(a);
		}
	}
	return 0;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data)
{
	unsigned int isUpdatedDummy;
	return readTupleUpdated(tableName, rid, data, &isUpdatedDummy);
}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data)
{
	unsigned int recFound = 0;
	unsigned int dataOffset = 0;
	unsigned int fieldOffset = 0;
	unsigned int fieldLen = 0;

	RID ridInTableCatalogDummy;
	vector<RID> ridsInColumnCatalogDummy;
	vector<Attribute_Internal> attrsInternal;
	void *buffer = malloc(PF_PAGE_SIZE);

	rc = findDetailsOfFieldsInATable(tableName, attrsInternal, ridInTableCatalogDummy ,ridsInColumnCatalogDummy);
	if(rc != success)
		return rc;

	rc = readTuple(tableName, rid, buffer);
	if(rc != success)
		return rc;

	for(unsigned int i = 0; i <= attrsInternal.size(); i++)
	{
		if(attrsInternal[i].isDeleted == 0)
		{
			if((attrsInternal[i].type == 0) || (attrsInternal[i].type == 1))
			{
				if((attributeName == attrsInternal[i].name) && (attrsInternal[i].isDeleted != 1))
				{
					recFound = 1;
					memcpy((char*)data, (char*)buffer+fieldOffset,4);
					dataOffset += 4;
				}
				fieldOffset += 4;
			}
			else if(attrsInternal[i].type == 2)
			{
				memcpy(&fieldLen, (char *)buffer+fieldOffset,4);
				if((attributeName == attrsInternal[i].name)  && (attrsInternal[i].isDeleted != 1))
				{
					recFound = 1;
					memcpy((char*)data, (char*)buffer+fieldOffset,4);
					dataOffset += 4;
					memcpy((char*)data+dataOffset, (char*)buffer+fieldOffset+4,fieldLen);
					dataOffset += fieldLen;
				}
				fieldOffset += 4+fieldLen;
			}
		}
	}
	if(recFound == 0)
		return -1;
	free(buffer);
	return 0;
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid)
{
	unsigned int data_size;
	PF_FileHandle fileHandle;
	PgNumOffset pgNumOffset;
	RID rid_in_updated_page;
	RID newRID;
	void *dataToBeInserted;
	void *buffer = malloc(PF_PAGE_SIZE);

	rc = fileHandle.OpenFile(tableName.c_str());
	if(rc != success)
		return rc;

	data_size = calculateTupleSize((void*)data,tableName);
	dataToBeInserted = malloc(data_size);

	rc = fileHandle.ReadPage(rid.pageNum, buffer);
	if(rc != success)
		return rc;

	rc = findRecordOffset(buffer ,rid, pgNumOffset);
	if(rc != success)
		return rc;

	if((pgNumOffset.PageNum != rid.pageNum) && (pgNumOffset.PageNum != 9999))
	{
		rid_in_updated_page.pageNum = pgNumOffset.PageNum;
		rid_in_updated_page.slotNum = pgNumOffset.SlotNum;
		rc = updateTuple(tableName, data, rid_in_updated_page);
		if(rc != success)
			return rc;
	}
	else if(pgNumOffset.PageNum != 9999)
	{
		if(data_size > pgNumOffset.recLength)
		{
			rc = insertTuple(tableName, data,newRID);
			if(rc != success)
				return rc;

			memcpy((char *)dataToBeInserted, &newRID.pageNum, sizeof(int));
			rc = fileHandle.WriteData(pgNumOffset.PageNum, pgNumOffset.slotOffset,sizeof(int),dataToBeInserted);
			if(rc != success)
				return rc;

			memcpy((char *)dataToBeInserted, &newRID.slotNum , sizeof(int));
			rc = fileHandle.WriteData(pgNumOffset.PageNum, pgNumOffset.slotOffset+4,sizeof(int),dataToBeInserted);
			if(rc != success)
				return rc;
		}
		else
		{
			rc = createEmptyRecordData(dataToBeInserted, pgNumOffset.recLength);
			if(rc != success)
				return rc;
			memcpy(dataToBeInserted, data, data_size);
			rc = fileHandle.WriteData(pgNumOffset.PageNum, pgNumOffset.OffsetInPage,pgNumOffset.recLength,dataToBeInserted);
			if(rc != success)
				return rc;
		}
	}
	else
		return -1;
	free(dataToBeInserted);
	free(buffer);
	fileHandle.CloseFile();
	return 0;
}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber)
{
	unsigned int totalFreeSpace = 0;
	unsigned int numRecsInPage;
	unsigned int recOffsetReorg = 0;
	unsigned int slotOffsetReorg = PF_PAGE_SIZE-12;
	unsigned int temp = 9999;
	unsigned int dataSize = 0;
	PgNumOffset pgNumOffset;
	RID rid;
	PF_FileHandle fileHandle;
	void *buffer = malloc(PF_PAGE_SIZE);
	void *reorg_buffer = malloc(PF_PAGE_SIZE);
	void *slotData = malloc(12);

	rc = fileHandle.OpenFile(tableName.c_str());
	if(rc != success)
		return rc;

	rc = fileHandle.ReadPage(pageNumber, buffer);
	if(rc != success)
		return rc;

	memcpy(&numRecsInPage,(char *)buffer+PF_PAGE_SIZE-12 ,4);
	memcpy((char *)slotData, &numRecsInPage, sizeof(int));
	memcpy((char *)slotData+4, &temp, sizeof(int));
	memcpy((char *)slotData+8, &temp, sizeof(int));
	memcpy((char*)reorg_buffer+slotOffsetReorg, (char*)slotData, 12);
	dataSize += 12;

	for(unsigned int i = 0; i < numRecsInPage; i++)
	{
		rid.pageNum = pageNumber;
		rid.slotNum = i;
		rc = findRecordOffset(buffer, rid, pgNumOffset);
		if(rc != success)
			return rc;
		slotOffsetReorg -= 12;

		if(pgNumOffset.PageNum != 9999)
		{
			void *recData = malloc(pgNumOffset.recLength);
			rc = readTuple(tableName, rid ,recData );
			if(rc != success)
				return rc;

			memcpy((char*)reorg_buffer+recOffsetReorg, (char*)recData, pgNumOffset.recLength);
			dataSize += pgNumOffset.recLength;
			memcpy((char *)slotData, &pageNumber, sizeof(int));
			memcpy((char *)slotData+4, &i, sizeof(int));
			memcpy((char *)slotData+8, &recOffsetReorg, sizeof(int));
			memcpy((char*)reorg_buffer+slotOffsetReorg, (char*)slotData, 12);
			recOffsetReorg += pgNumOffset.recLength;
			dataSize += 12;
			free(recData);
		}
		else
		{
			memcpy((char *)slotData, &temp, sizeof(int));
			memcpy((char *)slotData+4, &i, sizeof(int));
			memcpy((char *)slotData+8, &recOffsetReorg, sizeof(int));
			memcpy((char*)reorg_buffer+slotOffsetReorg, (char*)slotData, 12);
			dataSize += 12;
		}
	}
	memcpy((char*)reorg_buffer+PF_PAGE_SIZE-8, &recOffsetReorg, 4);
	totalFreeSpace = PF_PAGE_SIZE - dataSize;
	memcpy((char*)reorg_buffer+PF_PAGE_SIZE-4, &totalFreeSpace, 4);
	rc = fileHandle.WritePage(pageNumber, reorg_buffer);
	if(rc != success)
		return rc;
	rc = fileHandle.CloseFile();
	if(rc != success)
		return rc;
	free(slotData);
	free(buffer);
	free(reorg_buffer);
	return 0;
}

RC RM::addAttribute(const string tableName, const Attribute attr)
{

	int tid;
	int colPos;
	int tupleSize;
	RID rid;
	RID ridInTableCatalogDummy;
	vector<RID> ridsInColumnCatalogDummy;
	vector<Attribute_Internal> attrsDummy;
	void *tuple_temp = malloc(PF_PAGE_SIZE);
	void *tuple;

	tid = findTIDOfATable(tableName,ridInTableCatalogDummy);
	rc = findDetailsOfFieldsInATable(tableName, attrsDummy, ridInTableCatalogDummy ,ridsInColumnCatalogDummy);
	if(rc != success)
		return rc;

	colPos = attrsDummy.size()+1;
	prepareColumnCatalogTuple(tid, sizeof(attr.name) ,(attr.name).c_str(), attr.type, attr.length ,colPos, 0, tuple_temp, &tupleSize);
	tuple = malloc(tupleSize);
	memcpy(tuple, tuple_temp,tupleSize);
	rc = insertTuple(ColumnCatalog,tuple,rid);
	if(rc != success)
		return rc;

	free(tuple);
	free(tuple_temp);
	return rc;
}

RC RM::dropAttribute(const string tableName, const string attributeName)
{
	unsigned int fieldOffset = 0;
	unsigned int isBreak = 0;
	unsigned int fieldLen = 0;

	vector<RID> ridsInColumnCatalog;
	vector<Attribute_Internal> attrsInternal;
	RID ridInTableCatalogDummy;
	RID rid;
	void *data;
	void *buffer = malloc(PF_PAGE_SIZE);

	rc = findDetailsOfFieldsInATable(tableName, attrsInternal, ridInTableCatalogDummy ,ridsInColumnCatalog);
	if(rc != success)
		return rc;

	for(unsigned int i = 0; i < ridsInColumnCatalog.size(); i++)
	{
		rid = ridsInColumnCatalog[i];
		rc = readTuple(ColumnCatalog, rid, buffer);
		fieldLen = 0;
		fieldOffset = 0;
		for(unsigned int k = 0; k < NUM_OF_FIELDS_IN_COL_CATALOG; k++)
		{
			if(k == 0 || k==2 || k==3 || k==4 || k==5)
			{
				if(!((isBreak == 1) && (k == 5)))
					fieldOffset += 4;
			}
			else if(k == 1)
			{
				memcpy(&fieldLen, (char *)buffer+fieldOffset, sizeof(int));
				char* fieldNameData = new char[fieldLen + 1];
				fieldNameData[fieldLen] = '\0';
				memcpy(fieldNameData, (char *)buffer+fieldOffset+4 ,fieldLen);
				fieldOffset += 4+fieldLen;
				if(fieldNameData == attributeName)
					isBreak = 1;
				free(fieldNameData);
			}
		}
		if(isBreak == 1)
			break;
	}
	unsigned int isDeleted = 1;
	data = malloc(fieldOffset + 4);
	memcpy((char*)data, (char*)buffer, fieldOffset + 4);
	memcpy((char*)data+fieldOffset, &isDeleted ,4);

	rc = updateTuple(ColumnCatalog, data, rid);
	if(rc != success)
		return rc;

	free(data);
	free(buffer);
	return 0;
}

RC RM::reorganizeTable(const string tableName)
{
	const string tableName_temp = tableName + "_temp";
 	return 0;
}

//-----------------------------------------------INTERNAL METHODS--------------------------------------------------------------------------------//

unsigned int RM::findTIDOfATable(const string tableName, RID &rid_table_catalog)
{
	unsigned int tid = 0;
	unsigned int numOfPages ;
	unsigned int  i, j, k;
	unsigned int currPageNum = 9999;
	unsigned int breakBit = 0;
	unsigned int numRecsInPage;
	unsigned int offsetInPage;
	unsigned int recOffset;
	unsigned int fieldLen;

	void *buffer = malloc(PF_PAGE_SIZE);

	rid_table_catalog.pageNum = 9999;
	rid_table_catalog.slotNum = 9999;

	PF_FileHandle fileHandle;
	rc = pf->OpenFile(TableCatalog,fileHandle);
	if(rc != success)
	{
		free(buffer);
		return 0;
	}
	numOfPages = fileHandle.GetNumberOfPages();

	for(i = 0; i < numOfPages; i++)
	{
		rc = fileHandle.ReadPage(i, buffer);
		if(rc != success)
		{
			free(buffer);
			return 0;
		}
		numRecsInPage = 0;
		memcpy(&numRecsInPage, (char *)buffer+PF_PAGE_SIZE-12, sizeof(int));
		offsetInPage = 0;
		for(j = 0; j < numRecsInPage; j++)
		{
			recOffset = 0;
			memcpy(&recOffset, (char *)buffer+PF_PAGE_SIZE-12*(j+2)+8, sizeof(int));
			memcpy(&currPageNum, (char *)buffer+PF_PAGE_SIZE-12*(j+2), sizeof(int));
			offsetInPage = recOffset;
			if(currPageNum != 9999)
			{
				fieldLen = 0;
				for(k = 0; k < NUM_OF_FIELDS_IN_TBL_CATALOG; k++)
				{
					if(k == 0)
					{
						memcpy(&tid, (char *)buffer+offsetInPage, sizeof(int));
						offsetInPage += 4;
					}
					else if(k == 1)
					{
						memcpy(&fieldLen, (char *)buffer+offsetInPage, sizeof(int));
						char* fieldData = (char*)malloc((fieldLen+1)*sizeof(char));
						fieldData[fieldLen] = '\0';
						memcpy(fieldData, (char *)buffer+offsetInPage+4,fieldLen);
						if(strcmp (fieldData,tableName.c_str()) == 0)
						{
							rid_table_catalog.pageNum = i;
							rid_table_catalog.slotNum = j;
							breakBit = 1;
						}
						offsetInPage +=  4+fieldLen;
						free(fieldData);
					}
					else if(k == 2)
					{
						memcpy(&fieldLen, (char *)buffer+offsetInPage, sizeof(int));
						offsetInPage += 4+fieldLen;
					}
					if(breakBit == 1)
						break;
				}
				if(breakBit == 1)
					break;
			}
		}
		if(breakBit == 1)
			break;
	}
	rc = fileHandle.CloseFile();
	if(rc != success)
	{
		free(buffer);
		return 0;
	}
	free(buffer);
	return tid;
}

RC RM::findDetailsOfFieldsInATable(const string tableName, vector<Attribute_Internal> &attrs, RID &rid_table_catalog ,vector<RID> &rid_column_catalog)
{
	unsigned int tid;
	unsigned int numOfFields;
	unsigned int numRecsInPage;
	unsigned int pageNum;
	unsigned int numOfPages;
	unsigned int offsetInPage;
	unsigned int fieldLen;
	unsigned int matchFound;
	unsigned int fieldData;
	unsigned int i, j, k;

	Attribute_Internal FieldDetails;
	RID rid_column_catalog_temp;

	void *buffer;
	attrs.clear();
	PF_FileHandle fileHandle;
	tid = findTIDOfATable(tableName, rid_table_catalog);
	if(tid == 0)
		return -1;
	if(rid_table_catalog.pageNum != 9999)
	{
		numOfFields = 0;
		buffer = malloc(PF_PAGE_SIZE);
		rc = pf->OpenFile(ColumnCatalog,fileHandle);
		if(rc != success)
			return rc;
		numOfPages = fileHandle.GetNumberOfPages();
		if(numOfPages == 0)
			return -1;
		for(i = 0; i < numOfPages; i++)
		{
			rc = fileHandle.ReadPage(i, buffer);
			if(rc != success)
				return rc;
			memcpy(&numRecsInPage, (char *)buffer+PF_PAGE_SIZE-12, sizeof(int));
			offsetInPage = 0;
			for(j = 0; j < numRecsInPage; j++)
			{
				memcpy(&pageNum, (char *)buffer+PF_PAGE_SIZE-(12+(j+1)*12), sizeof(int));
				memcpy(&offsetInPage, (char *)buffer+PF_PAGE_SIZE-(12+(j+1)*12)+8, sizeof(int));
				if(pageNum != 9999)
				{
					fieldLen = 0;matchFound = 0;
					for(k = 0; k < NUM_OF_FIELDS_IN_COL_CATALOG; k++)
					{
						if(k == 0 || k==2 || k==3 || k==4 || k==5)
						{
							memcpy(&fieldData, (char *)buffer+offsetInPage, sizeof(int));
							if((k==0) && (tid == fieldData))
							{
								numOfFields++;
								rid_column_catalog_temp.pageNum = i;
								rid_column_catalog_temp.slotNum = j;
								matchFound = 1;
							}
							else if(matchFound == 1)
							{
								if(k==2)
									FieldDetails.type = AttrType(fieldData);
								else if(k==3)
									FieldDetails.length = fieldData;
								else if(k==4)
									FieldDetails.position = fieldData;
								else if(k==5)
									FieldDetails.isDeleted = fieldData;
							}
							offsetInPage += 4;
						}
						else if(k == 1)
						{
							memcpy(&fieldLen, (char *)buffer+offsetInPage, sizeof(int));
							char* fieldNameData = new char[fieldLen + 1];
							fieldNameData[fieldLen] = '\0';
							memcpy(fieldNameData, (char *)buffer+offsetInPage+4 ,fieldLen);
							offsetInPage += 4+fieldLen;
							if(matchFound == 1)
								FieldDetails.name = fieldNameData;
							delete [] fieldNameData;
						}
					}
					if(matchFound == 1)
					{
						rid_column_catalog.push_back(rid_column_catalog_temp);
						attrs.push_back(FieldDetails);
						matchFound = 0;
					}
				}
			}
		}
		free(buffer);
		rc = fileHandle.CloseFile();
		if(rc != success)
			return rc;
	}
	else
		return 1;

	return 0;
}

RC RM::findRecordOffset(const void* data, const RID &rid, PgNumOffset &pgNumOffset)//Ideally should return page + offset in that page
{
	unsigned int numOfSlots = 0;
	unsigned int slotOffset = 9999;
	unsigned int currSlotNum = 9999;
	unsigned int currPageNum = 9999;
	unsigned int recordOffset = 9999;
	unsigned int nextRecordOffset = 9999;
	unsigned int recordLength = 0;
	unsigned int lastPageEndOffset = 9999;

	pgNumOffset.PageNum = currPageNum;
	pgNumOffset.SlotNum = currSlotNum;
	pgNumOffset.OffsetInPage = recordOffset;
	pgNumOffset.slotOffset = slotOffset;
	pgNumOffset.recLength = recordLength;

	slotOffset = PF_PAGE_SIZE - (12);

	memcpy(&lastPageEndOffset , (char *)data+PF_PAGE_SIZE - (8),4);
	memcpy(&numOfSlots , (char *)data+slotOffset,4);

	if(numOfSlots >= rid.slotNum+1)
	{
		slotOffset -= (rid.slotNum+1)*12;
		memcpy(&currPageNum , (char *)data+slotOffset,4);
		memcpy(&currSlotNum , (char *)data+slotOffset+4,4);
		memcpy(&recordOffset , (char *)data+slotOffset+8,4);
		pgNumOffset.PageNum = currPageNum;
		pgNumOffset.SlotNum = currSlotNum;
		pgNumOffset.OffsetInPage = recordOffset;
		pgNumOffset.slotOffset = slotOffset;
		if(currPageNum != 9999)
		{
			if(rid.slotNum == numOfSlots-1)
				recordLength = lastPageEndOffset - pgNumOffset.OffsetInPage;
			else
			{
				memcpy(&nextRecordOffset , (char *)data+slotOffset-12+8,4);
				recordLength = nextRecordOffset - recordOffset;
			}
			pgNumOffset.recLength = recordLength;
		}
	}
	else
		return -1;
	return 0;
}

RC RM::createEmptyRecordData(const void* data, unsigned int recordLength)
{
	char* emptyRecord[recordLength+1];
	emptyRecord[recordLength] = '\0';
	for(unsigned int i = 0; i < recordLength ; i++)
		emptyRecord[i] = 0x00;
	memcpy((char *)data, emptyRecord, recordLength);
	return 0;
}

RC RM::createEmptySlotData(const void* data)
{
	const int emptySlotOffset = 9999;
	memcpy((char *)data, &emptySlotOffset, sizeof(int));
	return 0;
}

RC RM::deleteRecord(void *buffer, PgNumOffset &pgNumOffset)
{
	int temp;
	void * data = malloc(pgNumOffset.recLength);
	void * slot_data = malloc(sizeof(int));

	createEmptyRecordData(data,pgNumOffset.recLength);
	memcpy((char *)buffer+pgNumOffset.OffsetInPage, data, pgNumOffset.recLength);
	createEmptySlotData(slot_data);

	memcpy(&temp, slot_data, sizeof(int));
	memcpy((char *)buffer+pgNumOffset.slotOffset, slot_data, sizeof(int));

	free(slot_data);
	free(data);

	return 0;
}


RC RM::getAttribute(void *buffer, const RID &rid, vector<Attribute> &attrs,const string attributeName, void *data)
{
	unsigned int recOffset;
	unsigned int fieldLen = 0;
	unsigned int dataOffset = 0;
	unsigned int i;

	PgNumOffset pgNumOffset;
	rc = findRecordOffset(buffer,rid, pgNumOffset);
	if(rc != success)
		return rc;
	recOffset = pgNumOffset.OffsetInPage;

	for(i = 0; i < (unsigned int)attrs.size();  i++)
	{
			if((attrs[i].type == 0) || (attrs[i].type == 1))
			{
				if(strcmp(attrs[i].name.c_str(),attributeName.c_str()) == 0)
				{
					memcpy((char*)data+dataOffset, (char*)buffer+recOffset,4);
					dataOffset += 4;
				}
				recOffset += 4;
			}
			else if(attrs[i].type == 2)
			{
				memcpy(&fieldLen, (char *)buffer+recOffset,4);
				if(strcmp(attrs[i].name.c_str(),attributeName.c_str()) == 0)
				{
					memcpy((char*)data+dataOffset, (char*)buffer+recOffset,4);
					dataOffset += 4;
					memcpy((char*)data+dataOffset, (char*)buffer+recOffset+4,fieldLen);
					dataOffset += fieldLen;
				}
				recOffset += 4+fieldLen;
			}
	}
	return 0;
}

RC RM::readTupleUpdated(const string tableName, const RID &rid, void *data, unsigned int* isUpdated)
{
	unsigned int dataOffset = 0;
	unsigned int fieldOffset = 0;
	unsigned int fieldLen = 0;
	PgNumOffset pgNumOffset;
	RID ridInTableCatalogDummy;
	RID rid_in_updated_page;
	vector<RID> ridsInColumnCatalogDummy;
	vector<Attribute_Internal> attrsInternal;
	PF_FileHandle fileHandle;
	void *buffer;
	void *data_temp;

	*isUpdated = 0;
	rc = fileHandle.OpenFile(tableName.c_str());
	if(rc != success)
		return rc;

	if(fileHandle.GetNumberOfPages() == 0)
		return -1;

	buffer = malloc(PF_PAGE_SIZE);
	data_temp = malloc(PF_PAGE_SIZE);
	rc = fileHandle.ReadPage(rid.pageNum, buffer);
	if(rc != success)
		return rc;
	
	rc = findDetailsOfFieldsInATable(tableName, attrsInternal, ridInTableCatalogDummy ,ridsInColumnCatalogDummy);
	if(rc != success)
		return rc;

	rc = findRecordOffset(buffer ,rid, pgNumOffset);
	if(rc != success)
		return rc;

	if((pgNumOffset.PageNum != rid.pageNum) && (pgNumOffset.PageNum != 9999))
	{
		rid_in_updated_page.pageNum = pgNumOffset.PageNum;
		rid_in_updated_page.slotNum = pgNumOffset.SlotNum;
		rc = readTuple(tableName, rid_in_updated_page, data);
		if(rc != success)
			return rc;
		*isUpdated = 1;
	}
	else if(pgNumOffset.PageNum != 9999)
	{
		memcpy(data_temp , (char *)buffer+pgNumOffset.OffsetInPage,pgNumOffset.recLength);
		dataOffset = 0;	fieldOffset = 0;
		for(unsigned int i = 0; i < attrsInternal.size(); i++)
		{
				if((attrsInternal[i].type == 0) || (attrsInternal[i].type == 1))
				{
					if(attrsInternal[i].isDeleted == 0)
					{
						memcpy((char*)data+dataOffset, (char*)data_temp+fieldOffset,4);
						dataOffset += 4;
					}
					fieldOffset += 4;
				}
				else if(attrsInternal[i].type == 2)
				{
					memcpy(&fieldLen, (char *)data_temp+fieldOffset,4);
					if(attrsInternal[i].isDeleted == 0)
					{
						memcpy((char*)data+dataOffset, (char*)data_temp+fieldOffset,4);
						dataOffset += 4;
						memcpy((char*)data+dataOffset, (char*)data_temp+fieldOffset+4,fieldLen);
						dataOffset += fieldLen;
					}
					fieldOffset += 4+fieldLen;
				}
		}
	}
	else
		return -1;
		
	fileHandle.CloseFile();
	free (data_temp);
	free(buffer);
	
	return 0;
}


unsigned RM::calculateTupleSize(const void * tuple, const string tableName)
{
	OFFSET offset=0;
	vector<RID> ridsInColumnCatalogDummy;
	RID ridInTableCatalogDummy;
	vector<Attribute_Internal> Tableattrs;
	findDetailsOfFieldsInATable(tableName, Tableattrs, ridInTableCatalogDummy ,ridsInColumnCatalogDummy);
	for(unsigned i =0;i<Tableattrs.size();i++)
	{
		if(!Tableattrs[i].isDeleted)
		{
			if(Tableattrs[i].type==TypeInt)
			{
				offset+=4;
			}

			if(Tableattrs[i].type==TypeReal)
			{
				offset+=4;
			}
			if(Tableattrs[i].type==TypeVarChar)
			{
				unsigned length;
				memcpy (&length,(char *)tuple+offset,sizeof(int));
				offset+=sizeof(int);
				offset= offset + length;
			}
		}
	}
	return offset;
}
