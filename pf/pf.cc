#include "pf.h"

PF_Manager* PF_Manager::_pf_manager = 0;

PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}

PF_Manager::PF_Manager()
{

}

PF_Manager::~PF_Manager()
{
	delete  _pf_manager;
	_pf_manager = 0;
}

RC PF_Manager::CreateFile(const char *fileName)
{
	fstream fin;
	fstream fout;
	fin.open(fileName,fstream::in); //To check if the file already exists
	if(!fin.is_open())				//Create the file only if it doesn't exist
	{
		fout.open(fileName, fstream::out);
		if(fout.is_open())
		{
			fout.close();
			return 0;
		}
		else
		{
			return -1;
		}
	}
	else
	{
		fin.close();
		return -1;
	}
}


RC PF_Manager::DestroyFile(const char *fileName)
{
	RC err;
	err = remove(fileName);
	if(err != 0)
	{
		return -1;
	}
	return 0;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	//Opening the file and passing the file handle
	return fileHandle.OpenFile(fileName);
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	return fileHandle.CloseFile();
}

RC PF_Manager::DeleteAllPages(const char *fileName)
{
	try
	{
		ofstream delete_file;
		delete_file.open(fileName,ofstream::trunc | fstream::binary);
		delete_file.close();
	}
	catch (fstream::failure& e)
	{
		return -1;
	}

	return 0;
}

PF_FileHandle::PF_FileHandle()
{
	m_filehandle.exceptions ( fstream::eofbit | fstream::failbit | fstream::badbit );
}


PF_FileHandle::~PF_FileHandle()
{

}

void PF_FileHandle::GetFilehandle(std::fstream &handle)
{
	memcpy(&handle,&this->m_filehandle,4);

}
void PF_FileHandle::SetFileHandle(std::fstream &handle)
{
	memcpy(&this->m_filehandle,&handle,4);

}

RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if(pageNum >= GetNumberOfPages())
	{
		return -1;
	}
	try
	{
		//put read pointer at the start of given page number
		m_filehandle.seekg(pageNum*PF_PAGE_SIZE,fstream::beg);

		//read the page
		m_filehandle.read((char *)data,PF_PAGE_SIZE);
	}
	catch (fstream::failure& e)
	{
		return -1;
	}

	return 0;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if(pageNum >= GetNumberOfPages())
	{
		return -1;
	}
	try
	{
		m_filehandle.seekg(pageNum*PF_PAGE_SIZE,fstream::beg);
		m_filehandle.write((char *)data,PF_PAGE_SIZE);
		}
	catch (fstream::failure& e)
	{
		return -1;
	}

    return 0;
}

RC PF_FileHandle::ReadData(PageNum pageNum, OFFSET offset, const void *data,unsigned size)
{
	try
	{
		m_filehandle.seekg(((pageNum*PF_PAGE_SIZE)+offset),fstream::beg);
		m_filehandle.read((char *)data,size);
	}
	catch (fstream::failure& e)
	{
		return -1;
	}
    return 0;
}

RC PF_FileHandle::WriteData(PageNum pageNum, OFFSET offset,NumOfBytes numOfBytes, const void *data)
{
	try
	{
		//cout << "OFFSET:" <<(pageNum*PF_PAGE_SIZE)+offset << endl;
		m_filehandle.seekg(((pageNum*PF_PAGE_SIZE)+offset),fstream::beg);
		m_filehandle.write((char *)data,numOfBytes);
		}
	catch (fstream::failure& e)
	{
		return -1;
	}
    return 0;
}

RC PF_FileHandle::AppendPage(const void *data)
{
	try
	{
		m_filehandle.seekg(0, fstream::end);
		m_filehandle.write((char *)data,PF_PAGE_SIZE);
	}
	catch (fstream::failure& e)
	{
		return -1;
	}

    return 0;
}

RC PF_FileHandle::AppendNewPage()
{
	try
	{
		void *emptypage = malloc(PF_PAGE_SIZE);
		for(unsigned j = 0; j < PF_PAGE_SIZE; j++)
		{
			*((char *)emptypage + j) = ' ';
		}
		AppendPage(emptypage);
		free(emptypage);

		void *data = malloc(12);
		unsigned noOfSlots =0,offset =0,freeSpace= 4084;
		memcpy((char*)data,&noOfSlots,4);
		memcpy((char*)data+4,&offset,4);
		memcpy((char*)data+8,&freeSpace,4);

		WriteData(GetNumberOfPages()-1, 4084 , 12,data);

		free(data);
	}
	catch (fstream::failure& e)
	{
		return -1;
	}
	return 0;

}


RC PF_FileHandle::DeleteData(PageNum pageNum, OFFSET offset, NumOfBytes numOfBytes)
{
	try
		{
			void *emptyrecord = malloc(numOfBytes);
			for(unsigned j = 0; j < numOfBytes; j++)
			{
				*((char *)emptyrecord + j) = ' ';
			}
			PageNum pageNum = GetNumberOfPages()-1;
			WriteData(pageNum, offset , numOfBytes, emptyrecord);
			free(emptyrecord);
		}
		catch (fstream::failure& e)
		{
			return -1;
		}
		return 0;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
	long begin, end;
	end   = 0;
	begin = 0;

	try
	{
		m_filehandle.seekg(0, fstream::beg);
		begin = m_filehandle.tellg();
		m_filehandle.seekg(0, fstream::end);
		end = m_filehandle.tellg();
	}
	catch (fstream::failure& e)
	{
		return 0;
	}
    return ((end-begin)/PF_PAGE_SIZE);
}

RC PF_FileHandle::OpenFile(const char *fileName)
{
	try
	{
		m_filehandle.open(fileName,fstream::in | fstream::out | fstream::binary);
	}
	catch (fstream::failure& e)
	{
		return -1;
	}

	return 0;
}

RC PF_FileHandle::CloseFile()
{
	try
	{
		m_filehandle.close();
	}
	catch (fstream::failure& e)
	{
		return -1;
	}
	return 0;
}

RC PF_FileHandle::findFreeSpace(unsigned dataSize,PageNum &pagenum)
{
	unsigned int freeSpace;

	for(unsigned i =0;i<this->GetNumberOfPages();i++)
	{
		//read free space
		void *data1 = malloc(4);

		ReadData(i,PF_PAGE_SIZE-4,data1,4);
		memcpy(&freeSpace,data1,sizeof(int));
        
        free(data1);
        data1 = NULL;
        
		if(dataSize+12 <= freeSpace)
		{
			pagenum = i;
			return 0;
		}
	}
	return -1;
}

RC PF_FileHandle::isOpen()
{
	if(m_filehandle.is_open())
		return 0;
	else
		return -1;

}
