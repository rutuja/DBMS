#ifndef _pf_h_
#define _pf_h_

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstring>

using namespace std;

typedef int RC;
typedef unsigned PageNum;
typedef unsigned OFFSET;
typedef unsigned NumOfBytes;

#define PF_PAGE_SIZE 4096

class PF_FileHandle;

class PF_Manager
{
public:
    static PF_Manager* Instance();                                      // Access to the _pf_manager instance
        
    RC CreateFile    (const char *fileName);                            // Create a new file
    RC DestroyFile   (const char *fileName);                            // Destroy a file
    RC OpenFile      (const char *fileName, PF_FileHandle &fileHandle); // Open a file
    RC CloseFile     (PF_FileHandle &fileHandle);                       // Close a file
    RC DeleteAllPages(const char *fileName);                            // Deletes all pages in the file

protected:    
    PF_Manager();                                                       // Constructor
    ~PF_Manager   ();                                                   // Destructor

private:
    static PF_Manager *_pf_manager;
};


class PF_FileHandle
{
protected:
	fstream m_filehandle;															//fileHandle;

public:
    PF_FileHandle();                                                    // Default constructor
    ~PF_FileHandle();                                                   // Destructor

    RC ReadPage(PageNum pageNum, void *data);                           // Get a specific page
    RC WritePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC AppendPage(const void *data);                                    // Append a specific page
    unsigned GetNumberOfPages();                                        // Get the number of pages in the file
    RC AppendNewPage();                    								// Write a specific page
    RC ReadData(PageNum pageNum, OFFSET offset, const void *data,unsigned size);
    RC WriteData(PageNum pageNum, OFFSET offset, NumOfBytes numOfBytes, const void *data);     	// Write data into page
    RC DeleteData(PageNum pageNum, OFFSET offset, NumOfBytes numOfBytes);     // Delete data from a page
    RC OpenFile(const char *fileName);
    RC CloseFile();
    RC findFreeSpace(unsigned dataSize,PageNum &pagenum);
    RC isOpen();
    void GetFilehandle(std::fstream &handle);
    void SetFileHandle(std::fstream &handle);

    string tableName;

 };
 
 #endif
