
#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;


// The following functions use  the following 
// format for the passed data.
//    For int and real: use 4 bytes
//    For varchar: use 4 bytes for the length followed by
//                          the characters

struct Value {
    AttrType type;          // type of value               
    void     *data;         // value                       
};


struct Condition {
    string lhsAttr;         // left-hand side attribute                     
    CompOp  op;             // comparison operator                          
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

// Buffer size and character buffer size
const unsigned buffersize = 200;

class Iterator {
    // All the relational operators and access methods are iterators.
    public:
		//====================Added===============//
		string relName;//left
		string rightrelName;
		unsigned numAttrInRel;
		unsigned numAttrInRightRel;
		vector<Attribute> relAttrs;
		vector<Attribute> rightrelAttrs;

		int QE_CompareAttrs(CompOp compOp,void* leftAttr,void* rightAttr, AttrType leftAttrType ,AttrType rightAttrType);
		void QE_CombineTuple(void* combinedTuple, vector<void*> splittedLeftTuple, vector<Attribute> leftAttrs
												, vector<void*> splittedRightTuple, vector<Attribute> rightAttrs);
		//====================Added===============//
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RM &rm;
        RM_ScanIterator *iter;
        string tablename;
        vector<Attribute> attrs;
        vector<string> attrNames;
        
        TableScan(RM &rm, const string tablename,const char* alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs[i].name);
            }
            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
//            RC rc = rm.scan(tablename,"",NO_OP,NULL, attrNames, *iter);
            rm.scan(tablename,"",NO_OP,NULL, attrNames, *iter);
            // Store tablename

            this->tablename = tablename;
            if(alias) this->tablename = alias;

            //====================Added===============//
            this->relName = this->tablename;
            this->numAttrInRel = attrs.size();
            this->relAttrs = attrs;
            //====================Added===============//
        };
        
        ~TableScan()
        {
			if (iter != 0)
			{
				delete iter;
			}
			iter = 0;
		};
       
        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tablename,NULL,NO_OP,NULL, attrNames, *iter);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            return iter->getNextTuple(rid, data);
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;
            
            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RM &rm;
        IX_IndexScan *iter;
        IX_IndexHandle handle;
        IX_IndexHandle handle1;
        string tablename;
        vector<Attribute> attrs;
        
        IndexScan(RM &rm,  IX_IndexHandle &indexHandle, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);
                     
            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = string(alias);
            
            // Store Index Handle
            iter = NULL;

           this->handle.pf_FileHandle.tableName = indexHandle.pf_FileHandle.tableName;
           this->handle.pf_FileHandle.OpenFile(this->handle.pf_FileHandle.tableName.c_str());
           this->handle.IX_attrType = indexHandle.IX_attrType;

//           void* value = NULL;
//           setIterator(NO_OP, value);
           //====================Added===============//
           this->relName = this->tablename;
           this->numAttrInRel = attrs.size();
           this->relAttrs = attrs;
           //====================Added===============//
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator(CompOp compOp, void *value)
        {
            if(iter != NULL)
            {
                iter->CloseScan();
                delete iter;
            }
            iter = new IX_IndexScan();

            iter->OpenScan(handle, compOp, value);

        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            int rc = iter->GetNextEntry(rid);
            if(rc == 0)
            {
                rc = rm.readTuple(tablename.c_str(), rid, data);
            }
            return rc;
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~IndexScan() 
        {
            iter->CloseScan();
        };
};


class Filter : public Iterator {
    // Filter operator

    public:
		vector<void *>filteredData;

		Filter(Iterator *input,   const Condition &condition )
		{
			RM *rm_local = RM::Instance();
			vector <void * > splittedTuple;
			int splittedTupleIndex = -1;
			void * tuple = malloc(buffersize);
			input->getAttributes(relAttrs);
			for(unsigned i =0;i<relAttrs.size();i++)
			{
				splittedTupleIndex++;
				if(strcmp(relAttrs[i].name.c_str(),condition.lhsAttr.c_str())==0)
					i = relAttrs.size();
			}
			int k = 0;

			while(input->getNextTuple(tuple)==0)
			{
		    	rm_local->QE_splitTuple(tuple,relAttrs,splittedTuple);
				unsigned int length = 0;
		    	if(rm_local->compareAttribute(splittedTuple[splittedTupleIndex],condition.op,condition.rhsValue.data,condition.rhsValue.type,length))
				{
		    		k++;
		    		int offset = 0;
		    		for(unsigned i =0;i<relAttrs.size();i++)
		    		{
						if(relAttrs[i].type==TypeInt)
						{
							offset+=4;
						}

						if(relAttrs[i].type==TypeReal)
						{
							offset+=4;
						}
						if(relAttrs[i].type==TypeVarChar)
						{
							unsigned length1;
							memcpy (&length1,(char *)tuple+offset,sizeof(int));
							offset+=sizeof(int);
							offset= offset + length1;
						}
		    		}

		    		void * newTuple = malloc(offset+4);
					memcpy ((char*)newTuple,&offset,sizeof(int));
					memcpy ((char*)newTuple+4,(char *)tuple,offset);
					filteredData.push_back(newTuple);
				}
			}
		};
		~Filter()
		{

		};

		RC getNextTuple(void *data);
		// For attribute in vector<Attribute>, name it as rel.attr
		void getAttributes(vector<Attribute> &attrs) const{
			attrs = relAttrs;
		};

};


class Project : public Iterator {
    // Projection operator
    public:
	vector<void *>projectedData;

        Project(Iterator *input,  const vector<string> &attrNames)
        {
        	RM *rm_local = RM::Instance();
			vector<Attribute> attrs;
			vector <void * > splittedTuple;
			unsigned projectedTupleSize = 0;
			void * tuple = malloc(buffersize);
			void *projectedTuple = malloc(buffersize);
			vector<string> dummyattrNames;
			input->getAttributes(attrs);
			//cout<<attrs.size()<<endl;
		   //====================Added===============//
		   this->relName = input->relName;
		   this->numAttrInRel = attrs.size();
		   //====================Added===============//
			for(unsigned i = 0;i<attrNames.size();i++)
			{
				for(unsigned j = 0;j<attrs.size();j++)
				{
					if(attrs[j].name.compare(attrNames[i])==0)
					{
						this->relAttrs.push_back(attrs[j]);
					}
				}
			}

		   while(input->getNextTuple(tuple)!=QE_EOF)
        	{
        		rm_local->QE_splitTuple(tuple,attrs,splittedTuple);
        		rm_local->createProjectedTupleWithoutVarchar(splittedTuple,attrNames,attrs,projectedTuple,projectedTupleSize);
        		void * newTuple = malloc(projectedTupleSize+4);
				memcpy ((char*)newTuple,&projectedTupleSize,sizeof(int));
				memcpy ((char*)newTuple+4,(char *)projectedTuple,projectedTupleSize);
				projectedData.push_back(newTuple);
        	}

        };
        ~Project()
        {

        };
        
        RC getNextTuple(void *data)
        {
        	if(!projectedData.empty())
			{
				unsigned int size;
				memcpy(&size,(char *)projectedData[projectedData.size()-1],4);
				memcpy((char *)data,(char *)projectedData[projectedData.size()-1]+4,size);
				projectedData.pop_back();
				return 0;
			}
        	return QE_EOF;
        };
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
		for(unsigned j = 0;j<this->relAttrs.size();j++)
		{
			attrs.push_back(this->relAttrs[j]);
		}

        };
};



class NLJoin : public Iterator {
    // Nested-Loop join operator
    public:
		vector<void*> joinedRecords;
//		vector<void*>::iterator joinedRecordsIterator;

        NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~NLJoin()
		{
			for (unsigned int i = 0;  i < joinedRecords.size(); i++)
				free(joinedRecords[i]);
			joinedRecords.clear();
		};

        //====================Changed===============//
        RC getNextTuple(void *data);// {return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};


class INLJoin : public Iterator {
    // Index Nested-Loop join operator
    public:
		vector<void*> joinedRecords;

        INLJoin(Iterator *leftIn,                               // Iterator of input R
                IndexScan *rightIn,                             // IndexScan Iterator of input S
                const Condition &condition,                     // Join condition
                const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~INLJoin()
		{
			for (unsigned int i = 0;  i < joinedRecords.size(); i++)
				free(joinedRecords[i]);
			joinedRecords.clear();
		};
		
        //====================Changed===============//
        RC getNextTuple(void *data);// {return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};


class HashJoin : public Iterator {
    // Hash join operator
    public:
		vector<void*> joinedRecords;
        HashJoin(Iterator *leftIn,                                // Iterator of input R
                 Iterator *rightIn,                               // Iterator of input S
                 const Condition &condition,                      // Join condition
                 const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~HashJoin()
		{
			for (unsigned int i = 0;  i < joinedRecords.size(); i++)
				free(joinedRecords[i]);
			joinedRecords.clear();
		};
		
        //====================Changed===============//
        RC getNextTuple(void *data);// {return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        bool isFreeSpaceAvailable(vector<string>partitionNames,unsigned noOfPages);
};


class Aggregate : public Iterator {
    // Aggregation operator
    public:
		vector<void*> joinedRecords;
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op                                // Aggregate operation
        );

        // Extra Credit
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  Attribute gAttr,                              // The attribute over which we are grouping the tuples
                  AggregateOp op                                // Aggregate operation
        );     

        ~Aggregate(){};
        //====================Changed===============//
        RC getNextTuple(void *data);// {return QE_EOF;};
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
        template<class T> void QE_CalculateAggr(vector<void*> dataCopyVector,vector<Attribute> attrs,AggregateOp op, unsigned attrPos, T *aggrValue);
        void QE_getGroupByDetails(vector<void*> dataCopyVector,vector<Attribute> attrs,unsigned grpAttrPos,vector<vector<unsigned> > &groupByRecPositions);
};

#endif
