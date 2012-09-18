/*#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <fstream>
#include <iostream>
#include <cassert>

#include "rm.h"

using namespace std;

RM *rm = RM::Instance();
const int success = 0;

// Function to prepare the data in the correct form to be inserted/read/updated
void prepareTuple(const int name_length, const string name, const int age, const float height, const int salary, void *buffer, int *tuple_size)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &name_length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), name_length);
    offset += name_length;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    *tuple_size = offset;
}

void addDataInEmployeeTable(const string tablename)
{
	RID rid;
	int tuple_size =0 ;
	void *tuple = malloc(100);

	prepareTuple(6, "Peters", 24, 170.1, 5000, tuple, &tuple_size);
	RC rc = rm->insertTuple(tablename, tuple, rid);

	prepareTuple(6, "Victor", 22, 180.2, 6000, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);

	prepareTuple(6, "Thomas", 28, 187.3, 4000, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);

	prepareTuple(6, "Rutuja", 27, 171.4, 9000, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);

	prepareTuple(6, "Amitra", 29, 172.5, 7000, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);

}

// Function to parse the data in buffer and print each field
void printTuple(const void *buffer, const int tuple_size)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;

    int name_length = 0;
    memcpy(&name_length, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "name_length: " << name_length << endl;

    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, name_length);
    name[name_length] = '\0';
    offset += name_length;
    cout << "name: " << name << endl;

    int age = 0;
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;

    float height = 0.0;
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;

    int salary = 0;
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "salary: " << salary << endl;

    cout << "****Printing Buffer: End****" << endl << endl;
}

void prepareTupleAfterAdd(const int name_length, const char *name, const int age, const int height, const int salary, const int ssn, void *buffer, int *tuple_size)
{
    int offset=0;

    memcpy((char*)buffer + offset, &(name_length), sizeof(int));
    offset += sizeof(int);
    memcpy((char*)buffer + offset, name, name_length);
    offset += name_length;

    memcpy((char*)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)buffer + offset, &height, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)buffer + offset, &ssn, sizeof(int));
    offset += sizeof(int);

    *tuple_size = offset;
}

void printTupleAfterAdd( const void *buffer, const int tuple_size)
{
    int offset = 0;
    printf("****Printing Buffer: Start****\n");

    int name_length = 0;
    memcpy(&name_length, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    printf("name_length: %d\n", name_length);

    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, name_length);
    name[name_length] = '\0';
    offset += name_length;
    printf("name: %s\n", name);

    int age = 0;
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    printf("age: %d\n", age);

    int height = 0;
    memcpy(&height, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    printf("height: %d\n", height);

    int salary = 0;
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    printf("Salary: %d\n", salary);

    int ssn = 0;
    memcpy(&ssn, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    printf("SSN: %d\n", ssn);

    printf("****Printing Buffer: End****\n\n");
}

void printTupleAfterDrop( const void *buffer, const int tuple_size)
{
    int offset = 0;
    printf("****Printing Buffer: Start****\n");

    int name_length = 0;
    memcpy(&name_length, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    printf("name_length: %d\n", name_length);

    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, name_length);
    name[name_length] = '\0';
    offset += name_length;
    printf("name: %s\n", name);

    int age = 0;
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    printf("age: %d\n", age);

    float height = 0;
    memcpy(&height, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    printf("height: %f\n", height);

    printf("****Printing Buffer: End****\n\n");
}

// Create an employee table
void createTable(const string tablename)
{
    cout << "****Create Table " << tablename << " ****" << endl;

    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    attrs.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    RC rc = rm->createTable(tablename, attrs);
    assert(rc == success);
    cout << "****Table Created: " << tablename << " ****" << endl << endl;
}


void secA_0(const string tablename)
{
    // Functions Tested
    // 1. Get Attributes
    cout << "****In Test Case 0****" << endl;

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tablename, attrs);
    assert(rc == success);

    for(unsigned i = 0; i < attrs.size(); i++)
    {
        cout << "Attribute Name: " << attrs[i].name << endl;
        cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
        cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }
    return;
}

void secA_1(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions tested
    // 1. Create Table ** -- made separate now.
    // 2. Insert Tuple **
    // 3. Read Tuple **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << "****In Test Case 1****" << endl;

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    // Insert a tuple into a table
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    cout << "Insert Data:" << endl;
    printTuple(tuple, tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Given the rid, read the tuple from table
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc == success);

    cout << "Returned Data:" << endl;
    printTuple(data_returned, tuple_size);

    // Compare whether the two memory blocks are the same
    if(memcmp(tuple, data_returned, tuple_size) == 0)
    {
        cout << "****Test case 1 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 1 failed****" << endl << endl;
    }

    free(tuple);
    free(data_returned);
    return;
}


void secA_2(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Delete Tuple **
    // 3. Read Tuple
    cout << "****In Test Case 2****" << endl;

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    // Test Insert the Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    cout << "Insert Data:" << endl;
    printTuple(tuple, tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Test Delete Tuple
    rc = rm->deleteTuple(tablename, rid);
    assert(rc == success);

    // Test Read Tuple
    memset(data_returned, 0, 100);
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc != success);

    cout << "After Deletion." << endl;

    // Compare the two memory blocks to see whether they are different
    if (memcmp(tuple, data_returned, tuple_size) != 0)
    {
        cout << "****Test case 2 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 2 failed****" << endl << endl;
    }

    free(tuple);
    free(data_returned);
    return;
}


void secA_3(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert Tuple
    // 2. Update Tuple **
    // 3. Read Tuple
    cout << "****In Test Case 3****" << endl;

    RID rid;
    int tuple_size = 0;
    int tuple_size_updated = 0;
    void *tuple = malloc(100);
    void *tuple_updated = malloc(100);
    void *data_returned = malloc(100);

    // Test Insert Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);
    cout << "Original RID slot = " << rid.slotNum << endl;

    // Test Update Tuple
    prepareTuple(6, "Newman", age, height, 100, tuple_updated, &tuple_size_updated);
    rc = rm->updateTuple(tablename, tuple_updated, rid);
    assert(rc == success);
    cout << "Updated RID slot = " << rid.slotNum << endl;

    // Test Read Tuple
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc == success);
    cout << "Read RID slot = " << rid.slotNum << endl;

    // Print the tuples
    cout << "Insert Data:" << endl;
    printTuple(tuple, tuple_size);

    cout << "Updated data:" << endl;
    printTuple(tuple_updated, tuple_size_updated);

    cout << "Returned Data:" << endl;
    printTuple(data_returned, tuple_size_updated);

    if (memcmp(tuple_updated, data_returned, tuple_size_updated) == 0)
    {
        cout << "****Test case 3 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 3 failed****" << endl << endl;
    }

    free(tuple);
    free(tuple_updated);
    free(data_returned);
    return;
}


void secA_4(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Read Attributes **
    cout << "****In Test Case 4****" << endl;

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    // Test Insert Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Test Read Attribute
    rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
    assert(rc == success);

    cout << "Salary: " << *(int *)data_returned << endl;
    if (memcmp((char *)data_returned, (char *)tuple+18, 4) != 0)
    {
        cout << "****Test case 4 failed" << endl << endl;
    }
    else
    {
        cout << "****Test case 4 passed" << endl << endl;
    }

    free(tuple);
    free(data_returned);
    return;
}


void secA_5(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 0. Insert tuple;
    // 1. Read Tuple
    // 2. Delete Tuples **
    // 3. Read Tuple
    cout << "****In Test Case 5****" << endl;

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);
    void *data_returned1 = malloc(100);

    // Test Insert Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Test Read Tuple
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc == success);
    printTuple(data_returned, tuple_size);

    cout << "Now Deleting..." << endl;

    // Test Delete Tuples
    rc = rm->deleteTuples(tablename);
    assert(rc == success);

    // Test Read Tuple
    memset((char*)data_returned1, 0, 100);
    rc = rm->readTuple(tablename, rid, data_returned1);
    assert(rc != success);
    printTuple(data_returned1, tuple_size);

    if(memcmp(tuple, data_returned1, tuple_size) != 0)
    {
        cout << "****Test case 5 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 5 failed****" << endl << endl;
    }

    free(tuple);
    free(data_returned);
    free(data_returned1);
    return;
}

void secA_6(const string tablename)
{
    // Functions Tested
    // 1. Simple scan **
    cout << "****In Test Case 6****" << endl;

    RID rid;
    int tuple_size = 0;
    int num_records = 5;
    int q;
    void *tuple;
    void *data_returned = malloc(100);

    RID rids[num_records];
    vector<char *> tuples;

    RC rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        float height = (float)i;
        prepareTuple(6, "Tester", 20+i, height, 123, tuple, &tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr);
    rc = rm->scan(tablename, "", NO_OP, NULL, attributes, rmsi);
    assert(rc == success);

    cout << "Scanned Data:" << endl;

    while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
    {
    	memcpy (&q,(char *)data_returned,sizeof(int));
    	cout<<"Age: "<< q << endl;
        //cout << "Age: " << *(int *)data_returned << endl;
    }
    rmsi.close();

    // Deleta Table
    rc = rm->deleteTable(tablename);
    assert(rc == success);

    free(data_returned);
    for(int i = 0; i < num_records; i++)
    {
        free(tuples[i]);
    }

    return;
}

void secA_7(const char *tablename)
{
    // Functions Tested
    // 1. Reorganize Page **
    printf("****In Test Case 7****\n");
    RID rid;
    int tuple_size = 0;
    int num_records = 5;
    void *tuple;
    void *data_returned = malloc(100);
    int sizes[num_records];
    RID rids[num_records];
    vector<char *> tuples;
    int rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);
        // Test Insert Tuple
        prepareTuple(6, "Tester", 20+i, i, 123, tuple, &tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);
        tuples.push_back((char *)tuple);
        sizes[i] = tuple_size;
        rids[i] = rid;

    }
    printf("After Insertion!\n");
    //Delete a tuple and check
    rid.pageNum = 0;
    rid.slotNum = 1;
    rc = rm->deleteTuple(tablename, rid);
    assert(rc == success);
    int pageid = 0;
    rc = rm->reorganizePage(tablename, pageid);
    assert(rc == success);
    // Print out the tuples one by one
    int i = 0;
    for (i = 0; i < num_records; i++)
    {
        rc = rm->readTuple(tablename, rids[i], data_returned);
        if(i == 1)
        	assert (rc != success);
        else
        {
        assert(rc == success);
        printTuple(data_returned, tuple_size);
        //if any of the tuples are not the same as what we entered them to be ... there is a problem with the reorganization.
        if (memcmp(tuples[i], data_returned, sizes[i]) != 0)
        {
            printf("****Test case 7 failed****\n");
            break;
        }
        }
    }
    if(i == num_records)
    {
        printf("****Test case 7 passed****\n");
    }

    // Delete Table
    rc = rm->deleteTable(tablename);
    assert(rc == success);
    printf("****Test case 7 passed****\n");
    free(data_returned);
    for(i = 0; i < num_records; i++)
    {
        free(tuples[i]);
    }
    printf("****Test case 7 END****\n");
    return;
}

void secA_8(const char *tablename)
{
    // Functions Tested
    // 1. Reorganize Page **
    printf("****In Test Case 8****\n");

    RID rid;
    int tuple_size = 0;
    int num_records = 5;
    void *tuple;
    void *data_returned = malloc(100);

    int sizes[num_records];
    RID rids[num_records];
    vector<char *> tuples;

    int rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);

        // Test Insert Tuple
        prepareTuple(6, "Tester", 20+i, i, 123, tuple, &tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        sizes[i] = tuple_size;
        rids[i] = rid;
    }

    int pageid = 0; // Depends on which page the records are
    rc = rm->reorganizePage(tablename, pageid);
    assert(rc == success);

    // Print out the tuples one by one
    int i = 0;
    for (i = 0; i < num_records; i++)
    {
        rc = rm->readTuple(tablename, rids[i], data_returned);
        assert(rc == success);
        printTuple(data_returned, tuple_size);

        //if any of the tuples are not the same as what we entered them to be ... there is a problem with the reorganization.
        if (memcmp(tuples[i], data_returned, sizes[i]) != 0)
        {
            printf("****Test case 8 failed****\n");
            break;
        }
    }
    if(i == num_records)
    {
        printf("****Test case 8 passed****\n");
    }

    // Delete Table
    rc = rm->deleteTable(tablename);
    assert(rc == success);
    free(data_returned);
    for(i = 0; i < num_records; i++)
    {
        free(tuples[i]);
    }
    return;
}
void secA_10(const string tablename)
{
	  // Functions Tested
	    // 1. Scan with conditions. **
	    printf("****In Extra Credit Test Case 4****\n");

	    RID rid;
	    int tuple_size = 0;
	    int num_records = 5;
	    void *tuple;
	    void *data_returned = malloc(100);

	    RID rids[num_records];
	    vector<char *> tuples;

	    RC rc = 0;
	    for(int i = 0; i < num_records; i++)
	    {
	        tuple = malloc(100);

	        // Insert Tuple
	        float height = (float)i;
	        prepareTuple(6, "Tester", 20+i, height, 123, tuple, &tuple_size);
	        rc = rm->insertTuple(tablename, tuple, rid);
	        cout<<"rid.slotno"<<rid.slotNum<<endl;
	        assert(rc == success);

	        tuples.push_back((char *)tuple);
	        rids[i] = rid;
	    }
	    cout << "After Insertion!" << endl;

	    // Set up the iterator
	    RM_ScanIterator rmsi;
	    string attr = "Age";
	    vector<string> attributes;
	    attributes.push_back(attr);

	    int i = 25;

	    rc = rm->scan(tablename, "Age", LT_OP, &i, attributes, rmsi);
	    assert(rc == success);

	    cout << "Scanned Data:" << endl;
	    int q =0 ;
	    while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
	    {
			memcpy (&q,(char *)data_returned,sizeof(int));
			cout<<"Age: "<<q<<endl;

	//        cout << "Age: " << *(int *)data_returned << endl;
	    }
	    rmsi.close();

	    // Deleta Table
	    rc = rm->deleteTable(tablename);
	    assert(rc == success);

	    free(data_returned);
	    for(int i = 0; i < num_records; i++)
	    {
	        free(tuples[i]);
	    }

	    return;
}

void secA_11(const string tablename)
{
    // Functions Tested
    // 1. different scan queries **
    cout << "****In Test Case 6****" << endl;

    RID rid;
    int tuple_size = 0;
    int num_records = 10;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    RID rids[num_records];
    vector<char *> tuples;

    RC rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        prepareTuple(6, "Tester", 26+i, 101.3, 5000+(i*1000), tuple, &tuple_size);
        printTuple(tuple, tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Salary";
    vector<string> attributes;
    attributes.push_back(attr);

    int q =0;
    //-------------------------------------------------------------------------------------
		rc = rm->scan(tablename, "", NO_OP, NULL, attributes, rmsi);
		assert(rc == success);

		cout << "Scanned Data:" << endl;
		while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
		{
			memcpy (&q,(char *)data_returned,sizeof(int));
			cout<<"Age: "<<q<<endl;
		}
	   rmsi.close();

		// Deleta Table
		rc = rm->deleteTable(tablename);
		assert(rc == success);

		free(data_returned);
		for(int i = 0; i < num_records; i++)
		{
			free(tuples[i]);
		}
}

void secA_12(const string tablename)
{
    // Functions Tested
    // 1. different scan queries **
    cout << "****In Test Case 6****" << endl;

    RID rid;
    int tuple_size = 0;
    int num_records = 10;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    RID rids[num_records];
    vector<char *> tuples;

    RC rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        prepareTuple(6, "Tester", 26+i, 101.3+float(i), 5000+(i*1000), tuple, &tuple_size);
        printTuple(tuple, tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Height";
    vector<string> attributes;
    attributes.push_back(attr);
    float ii = 108.1;
     float qq =0.0;
  	//-------------------------------------------------------------------------------------
 	rc = rm->scan(tablename, "Height", LE_OP, &ii, attributes, rmsi);
 	assert(rc == success);

 	cout << "Scanned Data3:" << endl;
 	while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
 	{
 		memcpy (&qq,(char *)data_returned,sizeof(int));
 		cout<<"Height: "<<qq<<endl;
 	}

 	rc = rm->scan(tablename, "Height", GE_OP, &ii, attributes, rmsi);
 	assert(rc == success);

 	cout << "Scanned Data3:" << endl;
 	while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
 	{
 		memcpy (&qq,(char *)data_returned,sizeof(int));
 		cout<<"Height: "<<qq<<endl;
 	}

  rmsi.close();

	// Deleta Table
	rc = rm->deleteTable(tablename);
	assert(rc == success);

	free(data_returned);
	for(int i = 0; i < num_records; i++)
	{
		free(tuples[i]);
	}
}

void secA_13(const string tablename)
{
    // Functions Tested
    // 1. different scan queries **
    cout << "****In Test Case 6****" << endl;

    RID rid;
    int tuple_size = 0;
    int num_records = 10;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    RID rids[num_records];
    vector<char *> tuples;

    RC rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        prepareTuple(6, "Tester", 26+i, 101.3, 5000+(i*1000), tuple, &tuple_size);
        printTuple(tuple, tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    // Set up the iterator
    int sal;
    RM_ScanIterator rmsi;
    string attr = "Salary";
    vector<string> attributes;
    attributes.push_back(attr);
    const string name = "Rutuja";
   	void *value = malloc(10);
   	unsigned name_length = 6;
   	memcpy((char *)value, &name_length, sizeof(int));
   	memcpy((char *)value + name_length, name.c_str(), name_length);


	  //-------------------------------------------------------------------------------------

	rc = rm->scan(tablename, "EmpName", EQ_OP, value, attributes, rmsi);
	assert(rc == success);
	cout << "Scanned Data1:" << endl;
	while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
	{
		memcpy (&sal,(char *)data_returned,sizeof(int));
		cout<<"Salary: "<<sal<<endl;
	}
	//-------------------------------------------------------------------------------------
	rc = rm->scan(tablename, "EmpName", NE_OP, value, attributes, rmsi);
	assert(rc == success);

	cout << "Scanned Data6:" << endl;
	while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
	{
		memcpy (&sal,(char *)data_returned,sizeof(int));
		cout<<"Salary: "<<sal<<endl;
	}

	rmsi.close();

	// Deleta Table
	rc = rm->deleteTable(tablename);
	assert(rc == success);

	free(data_returned);
	for(int i = 0; i < num_records; i++)
	{
		free(tuples[i]);
	}
}

void secB_1(const char *tablename, const int name_length, const char *name, const int age, const int height, const int salary)
{
    //secB_1("tbl_employee", 6, name1, 24, 170, 5000);
    // Functions Tested
    // 1. Insert tuple
    // 2. Read Attributes
    // 3. Drop Attributes **
    printf("****In Extra Credit Test Case 1****\n");

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);
    void *data_returned1 = malloc(100);
    // Insert Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    int rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Read Attribute
    rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
    assert(rc == success);
    printf("Salary: %d\n", *(int *)data_returned);

    if(memcmp((char *)data_returned, (char *)tuple+18, 4) != 0)
    {
        printf("Read attribute failed!\n");
    }
    else
    {
		printf("Read attribute passed!\n");
		vector<Attribute> attrs;
		rm->getAttributes(tablename,attrs);
		for(int i = 0; i < (int)attrs.size();  i++)
			cout << attrs[i].name << " " << attrs[i].type << " " << attrs[i].length << endl;

		rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
		assert(rc == success);

		cout << "Salary: " << *(int *)data_returned << endl;

		// Drop the attribute
		rc = rm->dropAttribute(tablename, "Salary");
		assert(rc == success);

		rm->getAttributes(tablename,attrs);
		for(int i = 0; i < (int)attrs.size();  i++)
				cout << attrs[i].name << " " << attrs[i].type << " " << attrs[i].length << endl;

		rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
		assert(rc != success);

		cout << "Salary: " << *(int *)data_returned1 << endl;
		if (memcmp((char *)data_returned1, (char *)tuple+18, 4) == 0)
		{
			cout << "****Test case secB_1 failed" << endl << endl;
		}
		else
		{
			cout << "****Test case secB_1 passed" << endl << endl;
		}
		// Read Tuple and print the tuple
		rc = rm->readTuple(tablename, rid, data_returned);
		assert(rc == success);
		printTupleAfterDrop(data_returned, tuple_size);

		vector<Attribute> attrs1;
		rm->getAttributes(tablename,attrs1);
		for(int i = 0; i < (int)attrs1.size();  i++)
			cout << attrs1[i].name << " " << attrs1[i].type << " " << attrs1[i].length << endl;
	}
    free(tuple);
    free(data_returned);
    return;
}

void secB_2(const char *tablename, const int name_length, const char* name, const int age, const int height, const int salary, const int ssn)
{
    // Functions Tested
    // 1. Add Attribute **
    // 2. Insert Tuple
    printf("****In Extra Credit Test Case 2****\n");

    RID rid;
    int tuple_size=0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    // Test Add Attribute
    Attribute newAttr;
    newAttr.name = "SSN";
    newAttr.type = TypeInt;
    newAttr.length = 4;

    int rc = rm->addAttribute(tablename,newAttr);
    assert(rc == success);

    // Test Insert Tuple
    prepareTupleAfterAdd(name_length, name, age, height, salary, ssn, tuple, &tuple_size);
    rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Test Read Tuple
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc == success);

    printf("Insert Data:\n");
    printTupleAfterAdd(tuple, tuple_size);

    printf("Returned Data:\n");
    printTupleAfterAdd(data_returned, tuple_size);

    if (memcmp(data_returned, tuple, tuple_size) != 0)
    {
        printf("****Extra Credit Test Case 2 failed****\n\n");
    }
    else
    {
        printf("****Extra Credit Test Case 2 passed****\n\n");
    }

    free(tuple);
    free(data_returned);
    return;
}



void secB_3(const char *tablename)
{
    // Functions Tested
    // 1. Reorganize Table **
    printf("****In Extra Credit Test Case 3****\n");

    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    RID rid;
    int num_records = 200;
    RID rids[num_records];

    int rc = 0;
    for(int i=0; i < num_records; i++)
    {
        // Insert Tuple
        prepareTuple(6, "Tester", i, i, 123, tuple, &tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);
        rids[i] = rid;
    }



    // Reorganize Table
//    rc = rm->reorganizeTable(tablename);
//    assert(rc == success);


   for(int i = 0; i < num_records; i++)
    {
        // Read Tuple
        rc = rm->readTuple(tablename, rids[i], data_returned);
        assert(rc == success);

        // Print the tuple
        printTuple(data_returned, tuple_size);
    }

    // Delete the table
    rc = rm->deleteTable(tablename);
    assert(rc == success);

    free(tuple);
    free(data_returned);
    return;
}

void Tests_Basic()
{
      // GetAttributes
	    secA_0("tbl_employee");

	    // Insert/Read Tuple
	    secA_1("tbl_employee", 6, "Peters", 24, 170.1, 5000);

	    // Delete Tuple
	    secA_2("tbl_employee", 6, "Victor", 22, 180.2, 6000);

	    // Update Tuple
	    secA_3("tbl_employee", 6, "Thomas", 28, 187.3, 4000);

	    // Read Attributes
	    secA_4("tbl_employee", 6, "Veekay", 27, 171.4, 9000);

	    // Delete Tuples
	    secA_5("tbl_employee", 6, "Dillon", 29, 172.5, 7000);

	    // Simple Scan
	    createTable("tbl_employee6");
	    secA_6("tbl_employee6");

	    // Reorganize Page
	    createTable("tbl_employee7");
	    secA_7("tbl_employee7");

	    // Reorganize Page
	    createTable("tbl_employee8");
	    secA_8("tbl_employee8");

	    //scan
	    createTable("tbl_employee10");
	    secA_10("tbl_employee10");

		createTable("tbl_employee11");
		secA_11("tbl_employee11");

		createTable("tbl_employee12");
		secA_12("tbl_employee12");

		createTable("tbl_employee13");
		addDataInEmployeeTable("tbl_employee13");
		secA_13("tbl_employee13");

	    return;
}

void Tests_Optional()
{
	 char *name1 = (char *)malloc(20);
	 char *name2 = (char *)malloc(20);
	 strcpy(name1,"Ramya");
	 strcpy(name2,"Rutuja");

	 // Drop Attribute
	 createTable("tbl_employee_b1");
	 //secB_1("tbl_employee_b1", 5, name1, 24, 170, 5000);

	 //Add Attribute
	 createTable("tbl_employee_b2");
	 //secB_2("tbl_employee_b2", 6, name2, 22, 180, 6000, 999);

	 // Reorganize Table
	 createTable("tbl_employee_b3");
	 secB_3("tbl_employee_b3");

	 return;
}

int main()
{
    // Basic Functions
    cout << endl << "Test Basic Functions..." << endl;
    // Create Table
    createTable("tbl_employee");
    Tests_Basic();
    Tests_Optional();

    remove("tableCatalog");
    remove("columnCatalog");
	remove("tbl_employee");
	remove("tbl_employee6");
	remove("tbl_employee7");
	remove("tbl_employee8");
	remove("tbl_employee9");
	remove("tbl_employee10");
	remove("tbl_employee11");
	remove("tbl_employee12");
	remove("tbl_employee_b1");
	remove("tbl_employee_b2");
	remove("tbl_employee_b3");

    return 0;
}*/
