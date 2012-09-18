# include "qe.h"
#include <iostream>

//==========Iterator methods ================//
int Iterator::QE_CompareAttrs(CompOp compOp,void* leftAttr,void* rightAttr, AttrType leftAttrType ,AttrType rightAttrType)
{
	unsigned leftInt, rightInt;
	float leftFloat, rightFloat;
	char* leftStr;
	char* rightStr;
	unsigned leftAttrLen, rightAttrLen;
	if(leftAttrType == rightAttrType)
	{
		switch(leftAttrType)
		{
			case TypeInt:
			{
				memcpy(&leftInt,(char*)leftAttr,sizeof(int));
				memcpy(&rightInt,(char*)rightAttr,sizeof(int));
				break;
			}
			case TypeReal:
			{
				memcpy(&leftFloat,(char*)leftAttr,sizeof(int));
				memcpy(&rightFloat,(char*)rightAttr,sizeof(int));
				break;
			}
			case TypeVarChar:
			{
				memcpy(&leftAttrLen,(char*)leftAttr, sizeof(int));
				memcpy(&rightAttrLen,(char*)rightAttr, sizeof(int));
				leftStr = (char*)malloc(leftAttrLen+1);
				memcpy(leftStr,(char*)leftAttr+sizeof(int),leftAttrLen);
				leftStr[leftAttrLen] = '\0';
				rightStr = (char*)malloc(rightAttrLen+1);
				memcpy(rightStr,(char*)rightAttr+sizeof(int),rightAttrLen);
				rightStr[rightAttrLen] = '\0';
				break;
			}

		}
		switch(compOp)
		{
			case EQ_OP:
			{
				switch(leftAttrType)
				{
					case TypeInt:
					{
						if(leftInt == rightInt)
							return 0;
						break;
					}
					case TypeReal:
					{
						if(leftFloat == rightFloat)
							return 0;
						break;
					}
					case TypeVarChar:
					{
						if(strcmp(leftStr,rightStr)==0)
							return 0;
						break;
					}
				}
				break;
			}
			case LT_OP: // <
			{
				switch(leftAttrType)
				{
					case TypeInt:
					{
						if(leftInt < rightInt)
							return 0;
						break;
					}
					case TypeReal:
					{
						if(leftFloat < rightFloat)
							return 0;
						break;
					}
					case TypeVarChar:
					{
						if(strcmp(leftStr,rightStr) < 0)
							return 0;
						break;
					}
				}
				break;
			}
			case GT_OP: // >
			{
				switch(leftAttrType)
				{
					case TypeInt:
					{
						if(leftInt > rightInt)
							return 0;
						break;
					}
					case TypeReal:
					{
						if(leftFloat > rightFloat)
							return 0;
						break;
					}
					case TypeVarChar:
					{
						if(strcmp(leftStr,rightStr) > 0)
							return 0;
						break;
					}
				}
				break;
			}
			case LE_OP: // <=
			{
				switch(leftAttrType)
				{
					case TypeInt:
					{
						if(leftInt <= rightInt)
							return 0;
						break;
					}
					case TypeReal:
					{
						if(leftFloat <= rightFloat)
							return 0;
						break;
					}
					case TypeVarChar:
					{
						if(strcmp(leftStr,rightStr) <= 0)
							return 0;
						break;
					}
				}
				break;
			}
			case GE_OP: // >=
			{
				switch(leftAttrType)
				{
					case TypeInt:
					{
						if(leftInt >= rightInt)
							return 0;
						break;
					}
					case TypeReal:
					{
						if(leftFloat >= rightFloat)
							return 0;
						break;
					}
					case TypeVarChar:
					{
						if(strcmp(leftStr,rightStr) >= 0)
							return 0;
						break;
					}
				}
				break;
			}
			case NE_OP: // !=
			{
				switch(leftAttrType)
				{
					case TypeInt:
					{
						if(leftInt != rightInt)
							return 0;
						break;
					}
					case TypeReal:
					{
						if(leftFloat != rightFloat)
							return 0;
						break;
					}
					case TypeVarChar:
					{
						if(strcmp(leftStr,rightStr) != 0)
							return 0;
						break;
					}
				}
				break;
			}
			case NO_OP: // !=
			{
				return 0;
				break;
			}
			return 1;
		}
		return 1;
	}
	return 1;
}

void Iterator::QE_CombineTuple(void* combinedTuple, vector<void*> splittedLeftTuple, vector<Attribute> leftAttrs
		, vector<void*> splittedRightTuple, vector<Attribute> rightAttrs)
{
	unsigned i;
	unsigned totaloffset = 0;
	unsigned len = 0;
	for(i = 0; i < splittedLeftTuple.size(); i++)
	{
		len = 0;
		if((leftAttrs[i].type == TypeInt) || (leftAttrs[i].type == TypeReal))
		{
			memcpy((char*)combinedTuple+totaloffset, (char*)splittedLeftTuple[i], sizeof(int));
			totaloffset += sizeof(int);
		}
		else if(leftAttrs[i].type == TypeVarChar)
		{
			memcpy(&len, (char*)splittedLeftTuple[i], sizeof(int));
			memcpy((char*)combinedTuple+totaloffset,(char*)splittedLeftTuple[i], sizeof(int) + len);
			totaloffset += sizeof(int) + len;
		}
	}
	for(i = 0; i < splittedRightTuple.size(); i++)
	{
		len = 0;
		if((rightAttrs[i].type == TypeInt) || (rightAttrs[i].type == TypeReal))
		{
			memcpy((char*)combinedTuple+totaloffset, (char*)splittedRightTuple[i], sizeof(int));
			totaloffset += sizeof(int);
		}
		else if(rightAttrs[i].type == TypeVarChar)
		{
			memcpy(&len, (char*)splittedRightTuple[i], sizeof(int));
			memcpy((char*)combinedTuple+totaloffset,(char*)splittedRightTuple[i], sizeof(int) + len);
			totaloffset += sizeof(int) + len;
		}
	}
	return;
}
//==========Filter methods ================//

RC Filter::getNextTuple(void *data)
{
	if(!filteredData.empty())
	{
		unsigned int size;
		memcpy(&size,(char *)filteredData[filteredData.size()-1],4);
		memcpy((char *)data,(char *)filteredData[filteredData.size()-1]+4,size);
		filteredData.pop_back();
		return 0;
	}
	return QE_EOF;
}

//==========Project methods ================//

//=================NLJoin==================//
NLJoin::NLJoin(Iterator *leftIn,                             // Iterator of input R
        TableScan *rightIn,                           // TableScan Iterator of input S
        const Condition &condition,                   // Join condition
        const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
 )
{
	RM *rm = RM::Instance();
	this->relName = leftIn->relName;
	this->rightrelName = rightIn->relName;
	this->numAttrInRel = leftIn->numAttrInRel;
	this->numAttrInRightRel = rightIn->numAttrInRel;
	this->relAttrs = leftIn->relAttrs;
	this->rightrelAttrs = rightIn->relAttrs;
	vector<Attribute> left_attrs, right_attrs;
	vector<void*> leftDataCopyVector;
	vector<void*> rightDataCopyVector;
	//vector<void*>::iterator it;

	Attribute_Internal temp_left_attr_forsplit,temp_right_attr_forsplit;
	string leftrel, leftattr,rightrel,rightattr;
	unsigned leftAttrPos, rightAttrPos;
	leftIn->getAttributes(left_attrs);
	rightIn->getAttributes(right_attrs);
	unsigned i,j;
	void* leftdata;
	void* rightdata;
	void* data = malloc(buffersize);
	vector<void*> splittedDataLeft,splittedDataRight ;
	for(i = 0; i < left_attrs.size(); i++)
	{
		if((strcmp(condition.lhsAttr.c_str(),left_attrs[i].name.c_str())) == 0)
			leftAttrPos = i;
	}
	for(i = 0; i < right_attrs.size(); i++)
	{
		if((strcmp(condition.rhsAttr.c_str(),right_attrs[i].name.c_str())) == 0)
			rightAttrPos = i;
	}

	memset(data, 0X0, buffersize);
	while(leftIn->getNextTuple(data) != QE_EOF)
	{
		void* leftdata_temp = malloc(buffersize);
		memset(leftdata_temp, 0X0, buffersize);
		memcpy(leftdata_temp,data,buffersize);
		leftDataCopyVector.push_back(leftdata_temp);
		memset(data, 0X0, buffersize);

	}
	memset(data, 0X0, buffersize);
	while(rightIn->getNextTuple(data) != QE_EOF)
	{
		void* rightdata_temp = malloc(buffersize);
		memset(rightdata_temp, 0X0, buffersize);
		memcpy(rightdata_temp,data,buffersize);
		rightDataCopyVector.push_back(rightdata_temp);
		memset(data, 0X0, buffersize);

	}
	leftdata = malloc(buffersize);
	memset(leftdata, 0X0, buffersize);
	rightdata = malloc(buffersize);
	memset(rightdata, 0X0, buffersize);
	for(i=0; i < leftDataCopyVector.size(); i++ )
	{
		memcpy((char*)leftdata,(char*)leftDataCopyVector[i],buffersize);
		splittedDataLeft.clear();
		rm->QE_splitTuple(leftdata,left_attrs,splittedDataLeft);
		for(j=0; j < rightDataCopyVector.size(); j++ )
		{
			memcpy((char*)rightdata,(char*)rightDataCopyVector[j],buffersize);
			splittedDataRight.clear();
			rm->QE_splitTuple(rightdata,right_attrs,splittedDataRight);
			if(QE_CompareAttrs(condition.op,splittedDataLeft[leftAttrPos],splittedDataRight[rightAttrPos]
			               ,left_attrs[leftAttrPos].type, right_attrs[rightAttrPos].type) == 0)
			{
				//Combine the tuples
				void* combinedTuple = malloc(buffersize);
				memset(combinedTuple, 0X0, buffersize);
				QE_CombineTuple(combinedTuple, splittedDataLeft, left_attrs, splittedDataRight,right_attrs);
				this->joinedRecords.push_back(combinedTuple);
			}
			memset(rightdata, 0X0, buffersize);
		}
		memset(leftdata, 0X0, buffersize);
	}
	
	free(leftdata);
	free(rightdata);
	free(data);
	
	for (unsigned int i = 0; i < leftDataCopyVector.size(); i++)
		free(leftDataCopyVector[i]);
	leftDataCopyVector.clear();

	for (unsigned int i = 0; i < rightDataCopyVector.size(); i++)
		free(rightDataCopyVector[i]);
	rightDataCopyVector.clear();	
}

RC NLJoin::getNextTuple(void *data)
{
	if(!joinedRecords.empty())
	{
		memcpy((char*)data,(char*)joinedRecords[joinedRecords.size()-1],buffersize);
		free(joinedRecords[joinedRecords.size()-1]);
		joinedRecords.pop_back();
		return 0;
	}
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void NLJoin::getAttributes(vector<Attribute> &attrs) const
{
    unsigned i;
    vector<Attribute> temp_attrs;
    vector<Attribute> left_attrs;
    vector<Attribute> right_attrs;
    Attribute tempAttr;
    left_attrs = this->relAttrs;
    right_attrs = this->rightrelAttrs;
    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < this->numAttrInRel; i++)
    {
        string tmp = this->relName;
        tmp += ".";
        tmp += left_attrs[i].name;
        tempAttr.name = tmp;
        tempAttr.length = left_attrs[i].length;
        tempAttr.type = left_attrs[i].type;
        temp_attrs.push_back(tempAttr);
    }
    for(i = 0; i < this->numAttrInRightRel; i++)
    {
        string tmp = this->rightrelName;
        tmp += ".";
        tmp += right_attrs[i].name;
        tempAttr.name = tmp;
        tempAttr.length = right_attrs[i].length;
        tempAttr.type = right_attrs[i].type;
        temp_attrs.push_back(tempAttr);
    }
    attrs.clear();
    for(i = 0; i < temp_attrs.size(); ++i)
	{
    	attrs.push_back(temp_attrs[i]);
	}
    temp_attrs.clear();
}

//=================INLJoin==================//
INLJoin::INLJoin(Iterator *leftIn,                               // Iterator of input R
        IndexScan *rightIn,                             // IndexScan Iterator of input S
        const Condition &condition,                     // Join condition
        const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
)
{
	RM *rm = RM::Instance();
	this->relName = leftIn->relName;
	this->rightrelName = rightIn->relName;
	this->numAttrInRel = leftIn->numAttrInRel;
	this->numAttrInRightRel = rightIn->numAttrInRel;
	this->relAttrs = leftIn->relAttrs;
	this->rightrelAttrs = rightIn->relAttrs;
	vector<Attribute> left_attrs, right_attrs;
	vector<void*> leftDataCopyVector;
	vector<void*> rightDataCopyVector;
	Attribute_Internal temp_left_attr_forsplit,temp_right_attr_forsplit;
	string leftrel, leftattr,rightrel,rightattr;
	unsigned leftAttrPos, rightAttrPos;
	leftIn->getAttributes(left_attrs);
	rightIn->getAttributes(right_attrs);
	unsigned i,j;
	void* leftdata;
	void* rightdata;
	void* data = malloc(buffersize);
	vector<void*> splittedDataLeft,splittedDataRight ;
	for(i = 0; i < left_attrs.size(); i++)
	{
		if((strcmp(condition.lhsAttr.c_str(),left_attrs[i].name.c_str())) == 0)
			leftAttrPos = i;
	}
	for(i = 0; i < right_attrs.size(); i++)
	{
		if((strcmp(condition.rhsAttr.c_str(),right_attrs[i].name.c_str())) == 0)
			rightAttrPos = i;
	}

	memset(data, 0X0, buffersize);
	while(leftIn->getNextTuple(data) != QE_EOF)
	{
		void* leftdata_temp = malloc(buffersize);
		memset(leftdata_temp, 0X0, buffersize);
		memcpy(leftdata_temp,data,buffersize);
		leftDataCopyVector.push_back(leftdata_temp);
		memset(data, 0X0, buffersize);

	}
	void* value = NULL;
	rightIn->setIterator(NO_OP, value);
	while(rightIn->getNextTuple(data) != QE_EOF)
	{
		void* rightdata_temp = malloc(buffersize);
		memset(rightdata_temp, 0X0, buffersize);
		memcpy(rightdata_temp,data,buffersize);
		rightDataCopyVector.push_back(rightdata_temp);
		memset(data, 0X0, buffersize);

	}
	leftdata = malloc(buffersize);
	memset(leftdata, 0X0, buffersize);
	rightdata = malloc(buffersize);
	memset(rightdata, 0X0, buffersize);
	for(i=0; i < leftDataCopyVector.size(); i++ )
	{
		memcpy((char*)leftdata,(char*)leftDataCopyVector[i],buffersize);
		splittedDataLeft.clear();
		rm->QE_splitTuple(leftdata,left_attrs,splittedDataLeft);
		for(j=0; j < rightDataCopyVector.size(); j++ )
		{
			memcpy((char*)rightdata,(char*)rightDataCopyVector[j],buffersize);
			splittedDataRight.clear();
			rm->QE_splitTuple(rightdata,right_attrs,splittedDataRight);
			if(QE_CompareAttrs(condition.op,splittedDataLeft[leftAttrPos],splittedDataRight[rightAttrPos]
			               ,left_attrs[leftAttrPos].type, right_attrs[rightAttrPos].type) == 0)
			{
				//Combine the tuples
				void* combinedTuple = malloc(buffersize);
				memset(combinedTuple, 0X0, buffersize);
				QE_CombineTuple(combinedTuple, splittedDataLeft, left_attrs, splittedDataRight,right_attrs);
				this->joinedRecords.push_back(combinedTuple);
			}
			memset(rightdata, 0X0, buffersize);
		}
		memset(leftdata, 0X0, buffersize);
	}
	
	free(leftdata);
	free(rightdata);
	free(data);
	
	for (unsigned int i = 0; i < leftDataCopyVector.size(); i++)
		free(leftDataCopyVector[i]);
	leftDataCopyVector.clear();

	for (unsigned int i = 0; i < rightDataCopyVector.size(); i++)
		free(rightDataCopyVector[i]);
	rightDataCopyVector.clear();	

}

RC INLJoin::getNextTuple(void *data)
{
	if(!joinedRecords.empty())
	{
		memcpy((char*)data,(char*)joinedRecords[joinedRecords.size()-1],buffersize);
		free(joinedRecords[joinedRecords.size()-1]);
		joinedRecords.pop_back();
		return 0;
	}
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
    unsigned i;
    vector<Attribute> temp_attrs;
    vector<Attribute> left_attrs;
    vector<Attribute> right_attrs;
    left_attrs = this->relAttrs;
    right_attrs = this->rightrelAttrs;
    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < this->numAttrInRel; i++)
    {
        string tmp = this->relName;
        tmp += ".";
        tmp += left_attrs[i].name;
        left_attrs[i].name = tmp;
        temp_attrs.push_back(left_attrs[i]);
    }
    for(i = 0; i < this->numAttrInRightRel; i++)
    {
        string tmp = this->rightrelName;
        tmp += ".";
        tmp += right_attrs[i].name;
        right_attrs[i].name = tmp;
        temp_attrs.push_back(right_attrs[i]);
    }
    attrs.clear();
    for(i = 0; i < temp_attrs.size(); ++i)
	{
    	attrs.push_back(temp_attrs[i]);
	}
//    left_attrs.clear();
//    right_attrs.clear();
    temp_attrs.clear();
}

//=================HashJoin==================//
HashJoin::HashJoin(Iterator *leftIn,                                // Iterator of input R
        Iterator *rightIn,                               // Iterator of input S
        const Condition &condition,                      // Join condition
        const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
)
{
	RM *rm123 = RM::Instance();
	vector<Attribute> left_attrs, right_attrs;
	vector<string>R_partitions_h,S_partitions_h,partitions_h2;
	int noOfPartitions_h = 16,noOfPartitions_h2 = 8;
	unsigned leftAttrPosition, rightAttrPosition;
	void *data = malloc(buffersize),*Rdata = malloc(buffersize),*h2Data = malloc(buffersize);
	string 	R_h [16] = {"R_partition_h_1","R_partition_h_2","R_partition_h_3","R_partition_h_4","R_partition_h_5",
						"R_partition_h_6","R_partition_h_7","R_partition_h_8","R_partition_h_9","R_partition_h_10",
						"R_partition_h_11","R_partition_h_12","R_partition_h_13","R_partition_h_14","R_partition_h_15","R_partition_h_16"},
			S_h[16] = {"S_partition_h_1","S_partition_h_2","S_partition_h_3","S_partition_h_4","S_partition_h_5",
					"S_partition_h_6","S_partition_h_7","S_partition_h_8","S_partition_h_9","S_partition_h_10",
					"S_partition_h_11","S_partition_h_12","S_partition_h_13","S_partition_h_14","S_partition_h_15","S_partition_h_16"},
			h2[8] = {"S_partition_h2_1","S_partition_h2_2","S_partition_h2_3","S_partition_h2_4","S_partition_h2_5",
					"S_partition_h2_6","S_partition_h2_7","S_partition_h2_8"};
	left_attrs.clear();
	right_attrs.clear();
	leftIn->getAttributes(left_attrs);
	rightIn->getAttributes(right_attrs);

	this->relAttrs.clear();
	this->rightrelAttrs.clear();
	for(unsigned int i =0;i<left_attrs.size();i++)
	{
		this->relAttrs.push_back(left_attrs[i]);
	}
	for(unsigned int i =0;i<right_attrs.size();i++)
	{
		this->rightrelAttrs.push_back(right_attrs[i]);
	}

	AttrType attrType;
	//retrieving comparison attribute position
	for(unsigned i = 0; i < left_attrs.size(); i++)
	{
		if((strcmp(condition.lhsAttr.c_str(),left_attrs[i].name.c_str())) == 0)
		{
			leftAttrPosition = i;
			attrType = left_attrs[i].type;
		}
	}
	for(unsigned i = 0; i < right_attrs.size(); i++)
	{
		if((strcmp(condition.rhsAttr.c_str(),right_attrs[i].name.c_str())) == 0)
			rightAttrPosition = i;
	}


	//creating partition files
	for(int i =0;i<noOfPartitions_h;i++)
	{
		R_partitions_h.push_back(R_h[i]);
		rm123->createTable(R_h[i],left_attrs);
	}
	for(int i =0;i<noOfPartitions_h;i++)
	{
		S_partitions_h.push_back(S_h[i]);
		rm123->createTable(S_h[i],right_attrs);
	}
	for(int i =0;i<noOfPartitions_h2;i++)
	{
		partitions_h2.push_back(h2[i]);
		rm123->createTable(h2[i],right_attrs);
	}

	//CHANGED
	left_attrs.clear();
	right_attrs.clear();
	leftIn->getAttributes(left_attrs);
	rightIn->getAttributes(right_attrs);

	//partitioning phase
	//creating k partitions of R tuples based on hash function h (value%16)

	RID rid;
	while(leftIn->getNextTuple(data) != QE_EOF)
	{
		vector<void*> splittedDataLeft;
		rm123->QE_splitTuple(data,left_attrs,splittedDataLeft);
		int partition =0;
		if(attrType == TypeInt)
			partition = *(int *)((char *)splittedDataLeft[leftAttrPosition])%noOfPartitions_h;
		else
			partition = (int)(*(float *)((char *)splittedDataLeft[leftAttrPosition]))%noOfPartitions_h;

		rm123->insertTuple(R_partitions_h[partition],data,rid);
	}
	memset(data, 0X0, buffersize);
	//creating k partitions of S tuples based on hash function h (value%16)
	while(rightIn->getNextTuple(data) != QE_EOF)
	{

		vector<void*> splittedDataRight ;
		rm123->QE_splitTuple(data,right_attrs,splittedDataRight);
		int partition =0;
		if(attrType == TypeInt)
			partition = *(int *)((char *)splittedDataRight[rightAttrPosition])%noOfPartitions_h;
		else
			partition = (int)(*(float *)((char *)splittedDataRight[rightAttrPosition]))%noOfPartitions_h;
		
		rm123->insertTuple(S_partitions_h[partition],data,rid);
	}

	for(int l=0; l < noOfPartitions_h; l++ )
	{
		//scanning partition l of S tuples
	    TableScan *ts_S = new TableScan(*rm123, S_partitions_h[l]);
	    while(ts_S->getNextTuple(data)==0)
	    {
	    	if(this->isFreeSpaceAvailable(partitions_h2,numPages))
	    	{
				vector<void*> splittedDataRight ;
				rm123->QE_splitTuple(data,right_attrs,splittedDataRight);
				int partition;
				if(attrType == TypeInt)
					partition = *(int *)((char *)splittedDataRight[rightAttrPosition])%noOfPartitions_h2;
				else
					partition = (int)(*(float *)((char *)splittedDataRight[rightAttrPosition]))%noOfPartitions_h2;

			    rm123->insertTuple(partitions_h2[partition],data,rid);
	    	}
			memset(data, 0X0, buffersize);
	    }
	    delete ts_S;
	    ts_S = 0;

		//probing phase
	    TableScan *ts_R = new TableScan(*rm123, R_partitions_h[l]);

		memset(Rdata, 0X0, buffersize);
	    while(ts_R->getNextTuple(Rdata)==0)
	    {
	    	vector<void*> splittedDataLeft ;
	    	rm123->QE_splitTuple(Rdata,left_attrs,splittedDataLeft);
			int partition;
			if(attrType == TypeInt)
				partition = *(int *)((char *)splittedDataLeft[leftAttrPosition])%noOfPartitions_h2;
			else
				partition = (int)(*(float *)((char *)splittedDataLeft[leftAttrPosition]))%noOfPartitions_h2;

		    TableScan *ts_h2 = new TableScan(*rm123, partitions_h2[partition]);

		    while(ts_h2->getNextTuple(h2Data)==0)
		    {
		    	vector<void*> splittedDataRight;
		    	rm123->QE_splitTuple(h2Data,right_attrs,splittedDataRight);
				if(QE_CompareAttrs(condition.op,splittedDataLeft[leftAttrPosition],splittedDataRight[rightAttrPosition]
				               ,left_attrs[leftAttrPosition].type, right_attrs[rightAttrPosition].type) == 0)
				{
					void* combinedTuple = malloc(buffersize);
					memset(combinedTuple, 0X0, buffersize);
					QE_CombineTuple(combinedTuple, splittedDataLeft, left_attrs, splittedDataRight,right_attrs);
					this->joinedRecords.push_back(combinedTuple);
				}
				memset(h2Data, 0X0, buffersize);
			}
			delete ts_h2;
			ts_h2 = 0;
			
			memset(Rdata, 0X0, buffersize);
		}
		delete ts_R;
		ts_R = 0;
		
	    for(unsigned int k =0;k<partitions_h2.size();k++)
	    	rm123->deleteTuples(partitions_h2[k]);
	}
	
	while(!R_partitions_h.empty())
	{
		rm123->deleteTable(R_partitions_h[R_partitions_h.size()-1]);
		R_partitions_h.pop_back();
	}
	while(!S_partitions_h.empty())
	{
		rm123->deleteTable(S_partitions_h[S_partitions_h.size()-1]);
		S_partitions_h.pop_back();
	}
	while(!partitions_h2.empty())
	{
		rm123->deleteTable(partitions_h2[partitions_h2.size()-1]);
		partitions_h2.pop_back();
	}
	free(data);
	free(Rdata);
	free(h2Data);
}

bool HashJoin::isFreeSpaceAvailable(vector<string>partitionNames,unsigned noOfPages)
{
	unsigned currentnoOfPages = 0;
	for(unsigned i=0;i<partitionNames.size();i++)
	{
		PF_FileHandle filehandle;
		PF_Manager *pf = PF_Manager::Instance();
		pf->OpenFile(partitionNames[i].c_str(),filehandle);
		filehandle.GetNumberOfPages();
		currentnoOfPages = currentnoOfPages + filehandle.GetNumberOfPages();
		pf->CloseFile(filehandle);
	}
	if(currentnoOfPages<=noOfPages)
		return true;
	else
		return false;

}
RC HashJoin::getNextTuple(void *data)
{
	if(!joinedRecords.empty())
	{
		memcpy((char*)data,(char*)joinedRecords[joinedRecords.size()-1],buffersize);
		free(joinedRecords[joinedRecords.size()-1]);
		joinedRecords.pop_back();
		return 0;
	}
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void HashJoin::getAttributes(vector<Attribute> &attrs) const
{
    unsigned i;
    vector<Attribute> temp_attrs;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < this->relAttrs.size(); i++)
       temp_attrs.push_back(this->relAttrs[i]);
    for(i = 0; i < this->rightrelAttrs.size(); i++)
        temp_attrs.push_back(this->rightrelAttrs[i]);
    attrs.clear();
    for(i = 0; i < temp_attrs.size(); ++i)
    	attrs.push_back(temp_attrs[i]);
    temp_attrs.clear();
}

//=================Aggregate==================//
Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
        Attribute aggAttr,                            // The attribute over which we are computing an aggregate
        AggregateOp op                                // Aggregate operation
)
{
	this->relName = input->relName;
	this->rightrelName = input->relName;

	vector<Attribute> attrs;
	vector<void*> dataCopyVector;

	unsigned attrPos;
	input->getAttributes(attrs);
	unsigned i;

	void* data;
	void* aggrValue;

	unsigned aggrValueInt = 0;
	float aggrValueFloat = 0;

	for(i = 0; i < attrs.size(); i++)
	{
		if(strcmp(attrs[i].name.c_str(), aggAttr.name.c_str()) == 0)
			attrPos = i;
	}
	data = malloc(buffersize);
	memset(data, 0X0, buffersize);
	while(input->getNextTuple(data) != QE_EOF)
	{
		void* data_temp = malloc(buffersize);
		memset(data_temp, 0X0, buffersize);
		memcpy(data_temp,data,buffersize);
		dataCopyVector.push_back(data_temp);
		memset(data, 0X0, buffersize);
	}

	aggrValue= malloc(4);

	if(attrs[attrPos].type == TypeInt)
	{
		float tempFloat;
		if(op == AVG)
		{
			unsigned aggrCountInt;
			this->QE_CalculateAggr(dataCopyVector,attrs,SUM,attrPos, &aggrValueInt);
			this->QE_CalculateAggr(dataCopyVector,attrs,COUNT,attrPos, &aggrCountInt);
			tempFloat = (float)aggrValueInt/(float)aggrCountInt;
			memcpy((char*)aggrValue,&tempFloat,sizeof(int));
		}
		else
		{
			this->QE_CalculateAggr(dataCopyVector,attrs,op,attrPos, &aggrValueInt);
			tempFloat = (float)aggrValueInt;
			memcpy((char*)aggrValue,&tempFloat,sizeof(int));
		}
	}

	else if(attrs[attrPos].type == TypeReal)
	{
		this->QE_CalculateAggr(dataCopyVector,attrs,op,attrPos, &aggrValueFloat);
		memcpy((char*)aggrValue,&aggrValueFloat,sizeof(int));
	}

	this->joinedRecords.push_back(aggrValue);
}

Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
        Attribute aggAttr,                            // The attribute over which we are computing an aggregate
        Attribute gAttr,                              // The attribute over which we are grouping the tuples
        AggregateOp op                                // Aggregate operation
)
{
	RM *rm = RM::Instance();
	this->relName = input->relName;
	this->rightrelName = input->relName;

	vector<Attribute> attrs;
	vector<void*> dataCopyVector;
	vector<void*> grpByDataCopyVector;
	vector<vector<unsigned> > groupByRecPositions;
	unsigned attrPos;
	unsigned grpAttrPos;
	input->getAttributes(attrs);
	unsigned i;
	unsigned j;
	unsigned length;
	unsigned offset;
	void* grpAttrAggr;
	void* data;

	int aggrValueInt = 0;
	float aggrValueFloat = 0;
	attrPos = attrs.size();
	grpAttrPos = attrs.size();

	for(i = 0; i < attrs.size(); i++)
	{
		if(strcmp(attrs[i].name.c_str(), aggAttr.name.c_str()) == 0)
		{
			attrPos = i;
			break;
		}
	}
	for(i = 0; i < attrs.size(); i++)
	{
		if(strcmp(attrs[i].name.c_str(), gAttr.name.c_str()) == 0)
		{
			grpAttrPos = i;
			break;
		}
	}

	if((attrPos < attrs.size()) && (grpAttrPos < attrs.size()))
	{
		data = malloc(buffersize);
		memset(data, 0X0, buffersize);
		while(input->getNextTuple(data) != QE_EOF)
		{
			void* data_temp = malloc(buffersize);
			memset(data_temp, 0X0, buffersize);
			memcpy(data_temp,data,buffersize);
			dataCopyVector.push_back(data_temp);
			memset(data, 0X0, buffersize);
		}

		this->QE_getGroupByDetails(dataCopyVector,attrs,grpAttrPos,groupByRecPositions);

		for(i = 0; i < groupByRecPositions.size(); i++)
		{
			offset = 0;
			grpByDataCopyVector.clear();
			for(j = 0; j < groupByRecPositions[i].size(); j++)
			{
				if(j==0) //get the gAttrVal
				{
					void* data = malloc(buffersize);
					vector<void*> splittedData;
					memset(data, 0X0, buffersize);
					memcpy((char*)data,(char*)dataCopyVector[groupByRecPositions[i][j]],buffersize);
					splittedData.clear();
					rm->QE_splitTuple(data,attrs,splittedData);
					if((attrs[grpAttrPos].type == TypeInt) || (attrs[grpAttrPos].type == TypeReal))
					{
						grpAttrAggr = malloc(8);
						memset(grpAttrAggr,0,8);
						memcpy((char*)grpAttrAggr,(char*)splittedData[grpAttrPos] ,sizeof(int));
						offset= 4;
					}
					else if(attrs[grpAttrPos].type == TypeVarChar)
					{
						memcpy(&length,(char*)splittedData[grpAttrPos] ,sizeof(int));
						grpAttrAggr = malloc(4+length+4);
						memset(grpAttrAggr,0,4+length+4);
						memcpy((char*)grpAttrAggr,(char*)splittedData[grpAttrPos] ,sizeof(int)+length);
						offset= 4+ length;
					}
					free(data);
				}
				grpByDataCopyVector.push_back(dataCopyVector[groupByRecPositions[i][j]]);
			}

			if(attrs[attrPos].type == TypeInt)
			{
				float tempFloat;
				if(op == AVG)
				{
					unsigned aggrCountInt;
					this->QE_CalculateAggr(grpByDataCopyVector,attrs,SUM,attrPos, &aggrValueInt);
					this->QE_CalculateAggr(grpByDataCopyVector,attrs,COUNT,attrPos, &aggrCountInt);
					tempFloat = (float)aggrValueInt/(float)aggrCountInt;
					memcpy((char*)grpAttrAggr+offset,&tempFloat,sizeof(int));
				}
				else
				{
					this->QE_CalculateAggr(grpByDataCopyVector,attrs,op,attrPos, &aggrValueInt);
					tempFloat = (float)aggrValueInt;
					memcpy((char*)grpAttrAggr+offset,&tempFloat,sizeof(int));
				}
			}

			if((attrs[attrPos].type == TypeReal))
			{
				this->QE_CalculateAggr(grpByDataCopyVector,attrs,op,attrPos, &aggrValueFloat);
				memcpy((char*)grpAttrAggr+offset,&aggrValueFloat,sizeof(int));
			}

			this->joinedRecords.push_back(grpAttrAggr);
		}
	}
}

RC Aggregate::getNextTuple(void *data)
{
	if(!joinedRecords.empty())
	{
		memcpy((char*)data,(char*)joinedRecords[joinedRecords.size()-1],buffersize);
		free(joinedRecords[joinedRecords.size()-1]);
		joinedRecords.pop_back();
		return 0;
	}
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
    unsigned i;
    vector<Attribute> temp_attrs;
    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < this->numAttrInRel; ++i)
    {
        string tmp = this->relName;
        tmp += ".";
        tmp += attrs[i].name;
        attrs[i].name = tmp;
        temp_attrs.push_back(attrs[i]);
    }
    attrs.clear();
    for(i = 0; i < temp_attrs.size(); ++i)
	{
    	attrs.push_back(temp_attrs[i]);
	}
    temp_attrs.clear();
}

template<class T> void Aggregate::QE_CalculateAggr(vector<void*> dataCopyVector,vector<Attribute> attrs
												  ,AggregateOp op, unsigned attrPos, T *aggrValue)
{
	RM *rm = RM::Instance();
	unsigned i;
	unsigned count = 0;
	T dataT;
	T aggrValT = 0;
	void* data = malloc(buffersize);
	vector<void*> splittedData;
	memset(data, 0X0, buffersize);
	for(i=0; i < dataCopyVector.size(); i++ )
	{
		memcpy((char*)data,(char*)dataCopyVector[i],buffersize);
		splittedData.clear();
		rm->QE_splitTuple(data,attrs,splittedData);
		switch(op)// MIN = 0, MAX, SUM, AVG, COUNT
		{
			case MIN:
			{
				memcpy(&dataT, (char*)splittedData[attrPos], sizeof(int));
				if(i == 0)
					aggrValT = dataT;
				else if(dataT < aggrValT)
					aggrValT = dataT;
				break;
			}
			case MAX:
			{
				memcpy(&dataT, (char*)splittedData[attrPos], sizeof(int));
				if(i == 0)
					aggrValT = dataT;
				else if(dataT > aggrValT)
					aggrValT = dataT;
				break;
			}
			case SUM:
			{
				memcpy(&dataT, (char*)splittedData[attrPos], sizeof(int));
				aggrValT += dataT;
				break;
			}
			case AVG:
			{
				memcpy(&dataT, (char*)splittedData[attrPos], sizeof(int));
				aggrValT += dataT;
				count++;
				break;
			}
			case COUNT:
			{
				aggrValT += 1;
				break;
			}
		}
		memset(data, 0X0, buffersize);
	}
	if(op == AVG)
		aggrValT = aggrValT/(T)count;

	memcpy(aggrValue,&aggrValT,sizeof(int));
	return;
}

void Aggregate::QE_getGroupByDetails(vector<void*> dataCopyVector,vector<Attribute> attrs,unsigned grpAttrPos,vector<vector<unsigned> > &groupByRecPositions)
{
	RM *rm = RM::Instance();
	unsigned i,j,k, loop_i_ok,loop_j_ok;
	vector<unsigned> iFound;
	vector<void*> splittedDataLeft;
	vector<void*> splittedDataRight;
	void* leftdata;
	void* rightdata;
	vector<void*> leftDataCopyVector;
	vector<void*> rightDataCopyVector;
	vector<Attribute> left_attrs;
	vector<Attribute> right_attrs;
	vector<unsigned> groupByRecPosition;
	CompOp  op = EQ_OP;
	leftDataCopyVector = dataCopyVector;
	rightDataCopyVector = dataCopyVector;
	left_attrs = attrs;
	right_attrs = attrs;
	leftdata = malloc(buffersize);
	memset(leftdata, 0X0, buffersize);
	rightdata = malloc(buffersize);
	memset(rightdata, 0X0, buffersize);
	for(i=0; i < leftDataCopyVector.size(); i++ )
	{
		groupByRecPosition.clear();
		loop_i_ok = 1;
		for(k = 0; k < iFound.size(); k++)
		{
			if(i == iFound[k])
				loop_i_ok = 0;
		}
		if(loop_i_ok == 1)
		{
			iFound.push_back(i);
			groupByRecPosition.push_back(i);
			memcpy((char*)leftdata,(char*)leftDataCopyVector[i],buffersize);
			splittedDataLeft.clear();
			rm->QE_splitTuple(leftdata,left_attrs,splittedDataLeft);
			for(j=0; j < rightDataCopyVector.size() ; j++ )
			{
				loop_j_ok = 1;
				for(k = 0; k < iFound.size(); k++)
				{
					if(j == iFound[k])
						loop_j_ok = 0;
				}
				if(loop_j_ok == 1)
				{

					memcpy((char*)rightdata,(char*)rightDataCopyVector[j],buffersize);
					splittedDataRight.clear();
					rm->QE_splitTuple(rightdata,right_attrs,splittedDataRight);
					if(QE_CompareAttrs(op,splittedDataLeft[grpAttrPos],splittedDataRight[grpAttrPos]
								   ,left_attrs[grpAttrPos].type, right_attrs[grpAttrPos].type) == 0)
					{
						iFound.push_back(j);
						groupByRecPosition.push_back(j);
					}
					memset(rightdata, 0X0, buffersize);
				}
			}
			memset(leftdata, 0X0, buffersize);
		}
		if(groupByRecPosition.size() > 0)
			groupByRecPositions.push_back(groupByRecPosition);
	}
	return;
}
