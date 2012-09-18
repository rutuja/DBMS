/*#include <iostream>
#include <string>
#include <cassert>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>

#include "pf.h"

using namespace std;

const int success = 0;

// Check if a file exists
bool FileExists(string fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}

int PFTest_1(PF_Manager *pf)
{
    // Functions Tested:
    // 1. CreateFile
    cout << "****In PF Test Case 1****" << endl;

    RC rc;
    string fileName = "test1";

    // Create a file named "test1"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Create "test1" again, should fail
    rc = pf->CreateFile(fileName.c_str());
    assert(rc != success);

    //Destroy File
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    cout << "PF Test Case 1 Passed!" << endl << endl;

    return 0;
}

int PFTest_2(PF_Manager *pf)
{
    // Functions Tested:
    // 1. OpenFile
    // 2. AppendPage
    // 3. GetNumberOfPages
    // 4. WritePage
    // 5. ReadPage
    // 6. CloseFile
    // 7. DestroyFile
    cout << "****In PF Test Case 2****" << endl;

    RC rc;
    string fileName = "test2";

    // Create a file named "test2"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Open the file "test2"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    
    // Append the first page
    // Write ASCII characters from 32 to 125 (inclusive)
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.AppendPage(data);
    assert(rc == success);
   
    // Get the number of pages
    unsigned count = fileHandle.GetNumberOfPages();
    assert(count == (unsigned)1);

    // Update the first page
    // Write ASCII characters from 32 to 41 (inclusive)
    data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 10 + 32;
    }
    rc = fileHandle.WritePage(0, data);
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(0, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);
 
    // Close the file "test"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    // DestroyFile
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);
    
    cout << "PF Test Case 2 Passed!" << endl << endl;

    return 0;
}

int PFTest_3(PF_Manager *pf){
	// Functions Tested:
	// 1. CreateFile
	cout << "****In PF Test Case 3****" << endl;
    RC rc;
    string fileName = "test3";

    // Destroy a file named "test3" before creating it

    rc = pf->DestroyFile(fileName.c_str());
    assert(rc != success);

    // Create a file named "test3"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Destroy "test3"
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    cout << "PF Test Case 3 Passed!" << endl << endl;

    return 0;
}

int PFTest_4(PF_Manager *pf){
	// Functions Tested:
	// 1. OpenFile
	// 2. CloseFile
	// 3. DestroyFile
	cout << "****In PF Test Case 4****" << endl;
    RC rc;
    string fileName = "test4";
    PF_FileHandle fileHandle;
    PF_FileHandle fileHandle1;

    //Opening a non existent file
	rc = pf->OpenFile(fileName.c_str(),fileHandle);
	assert(rc != success);

	//Create the file named "test4"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    //Opening the fileHandle
	rc = pf->OpenFile(fileName.c_str(),fileHandle);
	assert(rc == success);

    //Opening the same fileHandle
	rc = pf->OpenFile(fileName.c_str(),fileHandle);
	assert(rc != success);

	//Opening the same file using different fileHandle
	rc = pf->OpenFile(fileName.c_str(),fileHandle1);
	assert(rc == success);

    //Close fileHandle
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    //Close fileHandle1
    rc = pf->CloseFile(fileHandle1);
    assert(rc == success);

    //Closing already closed fileHandle
    rc = pf->CloseFile(fileHandle);
    assert(rc != success);

	//Destroy the file with no open fileHandles
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    cout << "PF Test Case 4 Passed!" << endl << endl;

    return 0;
}

int PFTest_5(PF_Manager *pf){
	// Functions Tested:
    // 1. AppendPage
	// 2. GetNumberOfPages
	cout << "****In PF Test Case 5****" << endl;
    RC rc;
    string fileName = "test5";

    // Create the file named "test5"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Open the file "test5"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    //Append 95 pages each of size 'PF_PAGE_SIZE'
    for(unsigned i = 32; i < 127; i++)
    {
    	void *data = malloc(PF_PAGE_SIZE);
    	for(unsigned j = 0; j < PF_PAGE_SIZE; j++)
    	{
    		*((char *)data+j) = i;
    	}
    	rc = fileHandle.AppendPage(data);
    	assert(rc == success);
    	free(data);
    }

    //get number of pages
    unsigned np = 0;
    np = fileHandle.GetNumberOfPages();
    assert(np == (unsigned)95);

    //close the file
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    //open the file "test5" again for appending data
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    //Append 5 pages each of size 'PF_PAGE_SIZE'
    for(unsigned i = 10; i < 15; i++)
    {
    	void *data = malloc(PF_PAGE_SIZE);
    	for(unsigned j = 0; j < PF_PAGE_SIZE; j++)
    	{
    		*((char *)data+j) = i;
    	}
    	rc = fileHandle.AppendPage(data);
    	assert(rc == success);
    	free(data);
    }

    //get number of pages
    np = fileHandle.GetNumberOfPages();
    assert(np == (unsigned)100);

    //Append 10 pages each of size 'PF_PAGE_SIZE'
    for(unsigned i = 48; i < 58; i++)
    {
    	void *data = malloc(PF_PAGE_SIZE);
    	for(unsigned j = 0; j < PF_PAGE_SIZE; j++)
    	{
    		*((char *)data+j) = i;
    	}
    	rc = fileHandle.AppendPage(data);
    	assert(rc == success);
    	free(data);
    }

    //get number of pages
    np = fileHandle.GetNumberOfPages();
    assert(np == (unsigned)110);

    //close the file
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    //destroy file
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);
    cout << "PF Test Case 5 Passed!" << endl << endl;
    return 0;
}

int PFTest_6(PF_Manager *pf){
	// Functions Tested:
    // 1. ReadPage
	cout << "****In PF Test Case 6****" << endl;
    RC rc;
    string fileName = "test6";

    // Create the file named "test6"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Open the file "test6"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    //Append 10 pages each of size 'PF_PAGE_SIZE'
    for(unsigned i = 10; i < 20; i++)
    {
    	void *data = malloc(PF_PAGE_SIZE);
    	for(unsigned j = 0; j < PF_PAGE_SIZE; j++)
    	{
    		*((char *)data+j) = i;
    	}
    	rc = fileHandle.AppendPage(data);
    	assert(rc == success);
    	free(data);
    }

    //get number of pages
    unsigned np;
    np = fileHandle.GetNumberOfPages();
    assert(np == (unsigned)10);

    // Read the first page
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(0, buffer);
    assert(rc == success);

    // Check the integrity of the page
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = 10;
    }
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);

    //close the file
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    //destroy file
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    cout << "PF Test Case 6 Passed!" << endl << endl;

    return 0;
}

int PFTest_7(PF_Manager *pf){
	// Functions Tested:
    // 1. WritePage
	cout << "****In PF Test Case 7****" << endl;
    RC rc;
    string fileName = "test7";

    // Create the file named "test7"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Open the file "test7"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    //Append 10 pages each of size 'PF_PAGE_SIZE'
    for(unsigned i = 48; i < 58; i++)
    {
    	void *data = malloc(PF_PAGE_SIZE);
    	for(unsigned j = 0; j < PF_PAGE_SIZE; j++)
    	{
    		*((char *)data + j) = i;
    	}
    	rc = fileHandle.AppendPage(data);
    	assert(rc == success);
    	free(data);
    }

    void *data = malloc(PF_PAGE_SIZE);
    {
    	for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    	{
    		*((char *)data+i) = 82;
    	}
    }

    //Write data to page 10 - should fail
    rc = fileHandle.WritePage(10, data);
    assert(rc != success);

    //Write data to page 5
    rc = fileHandle.WritePage(5, data);
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(5, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);

    // Close the file "test7"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    // DestroyFile
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);

    cout << "PF Test Case 7 Passed!" << endl << endl;

    return 0;
}

int PFTest_8(PF_Manager *pf){
	// Functions Tested:
    // 1. WritePage
	cout << "****In PF Test Case 8****" << endl;
    RC rc;
    string fileName = "test8";

    // Create the file named "test8"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Open the file "test8"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    rc = fileHandle.AppendNewPage();
    assert(rc == success);

    void *data = malloc(PF_PAGE_SIZE);


		for(int i=0;i<PF_PAGE_SIZE;i++)
		*((char *)data + i) = 'A';

		*((char *)data + 4088) = '0';
		*((char *)data + 4089) = '0';
		*((char *)data + 4090) = '0';
		*((char *)data + 4091) = '0';
    	*((char *)data + 4092) = '4';
    	*((char *)data + 4093) = '0';
    	*((char *)data + 4094) = '8';
    	*((char *)data + 4095) = '8';

    	rc = fileHandle.AppendPage(data);
    	assert(rc == success);

 //       rc = fileHandle.AppendNewPage();
  //      assert(rc == success);

    // Read the page 1
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(1, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);

    //cout << buffer <<endl;

    fileHandle.DeleteData(1,PF_PAGE_SIZE-10, 5);
    rc = fileHandle.ReadPage(1, buffer);
    //Convert bytes to a zero-terminated string
    char *lpszCopy;
    lpszCopy = new char[PF_PAGE_SIZE + 1];
    memcpy(lpszCopy, buffer, PF_PAGE_SIZE);
    lpszCopy[PF_PAGE_SIZE] = '\0';
    printf("'%s'\n", lpszCopy);

    // Close the file "test8"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);
    // Read the page 1

    pf->DeleteAllPages(fileName.c_str());

    cout << "PF Test Case 8 Passed!" << endl << endl;
    free(buffer);
    free(data);
    return 0;
}

int PFTest_9(PF_Manager *pf){
	// Functions Tested:
    // 1. WritePage
	cout << "****In PF Test Case 9****" << endl;
    RC rc;
    string fileName = "test9";

    // Create the file named "test9"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    // Open the file "test9"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    rc = fileHandle.AppendNewPage();
    assert(rc == success);

    void *data = malloc(PF_PAGE_SIZE);


		for(int i=0;i<PF_PAGE_SIZE;i++)
		{
			*((char *)data + i) = 'A';
		}


    	rc = fileHandle.AppendPage(data);
    	assert(rc == success);

 //       rc = fileHandle.AppendNewPage();
  //      assert(rc == success);

    // Read the page 1
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(1, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);

    //cout << buffer <<endl;

    fileHandle.DeleteData(1,PF_PAGE_SIZE-10, 5);
    //void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(1, buffer);
    //Convert bytes to a zero-terminated string
    char *lpszCopy;
    lpszCopy = new char[PF_PAGE_SIZE + 1];
    memcpy(lpszCopy, buffer, PF_PAGE_SIZE);
    lpszCopy[PF_PAGE_SIZE] = '\0';
    printf("'%s'\n", lpszCopy);

    // Close the file "test8"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);
    // Read the page 1

    pf->DeleteAllPages(fileName.c_str());

    cout << "PF Test Case 9 Passed!" << endl << endl;
    free(buffer);
    free(data);
    return 0;
}
int main()
{
    PF_Manager *pf = PF_Manager::Instance();
    RC rc;
    remove("test1");
    remove("test2");
    remove("test3");
    remove("test4");
    remove("test5");
    remove("test6");
    remove("test7");
    remove("test8");
    remove("test9");

    //PF Test Case 1 Begin
    rc = PFTest_1(pf);
    //PF Test Case 1 End

    //PF Test Case 2 Begin
    rc = PFTest_2(pf);
    //PF Test Case 2 End

    //PF Test Case 3 Begin
    rc = PFTest_3(pf);
    //PF Test Case 3 End
    
    //PF Test Case 4 Begin
    rc = PFTest_4(pf);
    //PF Test Case 4 End

    //PF Test Case 5 Begin
    rc = PFTest_5(pf);
    //PF Test Case 5 End

    //PF Test Case 6 Begin
    rc = PFTest_6(pf);
    //PF Test Case 6 End

    //PF Test Case 7 Begin
    rc = PFTest_7(pf);
    //PF Test Case 7 End

    rc = PFTest_8(pf);

    rc = PFTest_9(pf);
    return 0;
}
*/
