#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
//Delete after testing
#include <iostream>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
//    cout << "Creating File at " << *f_path << endl;
    if(f_type==heap){
        diskFile.Open(0, const_cast<char *>(f_path));
        pageIndex = 0;
        isWriting = 0;
        MoveFirst();
    }
//    cout << "File Created "<< endl;
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    ComparisonEngine comp;

    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
            this->Add(temp);
    }
    if (isWriting == 1)
        diskFile.AddPage(&bufferPage, pageIndex);
}

int DBFile::Open (const char *f_path) {
    //Just for testing
    diskFile.Open(1, const_cast<char *>(f_path));
    pageIndex = 0;
    //Reading mode on default
    isWriting = 0;
    MoveFirst();
    return 1;
}
//Stop writing mode and do move first
void DBFile::MoveFirst () {
    if (isWriting == 1) {
        diskFile.AddPage(&bufferPage, pageIndex);
        isWriting = 0;
    }
    pageIndex = 0;
    bufferPage.EmptyItOut();
    //If DBfile is not empty
    if (diskFile.GetLength() > 0) {
        diskFile.GetPage(&bufferPage, pageIndex);
    }
    //Delete After testing
    cout << "length of file is " << diskFile.GetLength() << "\n";
}
int DBFile::Close () {
    if (isWriting == 1)
        diskFile.AddPage(&bufferPage, pageIndex);
    diskFile.Close();
    cout << "Closing file, length of file is " << diskFile.GetLength() << "Pages" << "\n";
    return 1;
}
//Assume file is open
void DBFile::Add (Record &rec) {
    //If file is reading, then empty buffer page and redirect to last page of file
    if (isWriting == 0) {
        bufferPage.EmptyItOut();
        //If file is not empty
        if (diskFile.GetLength() > 0) {
            diskFile.GetPage(&bufferPage, diskFile.GetLength() - 2);
            pageIndex = diskFile.GetLength() - 2;
        }
        isWriting = 1;
    }
    //If reach the end of page
    if (bufferPage.Append(&rec) == 0) {
        diskFile.AddPage(&bufferPage, pageIndex++);
        bufferPage.EmptyItOut();
        bufferPage.Append(&rec);
    }
}
//Assume file is open
int DBFile::GetNext (Record &fetchme) {
    //If file is writing, then write page into disk based file, redirect page ot first page
    if (isWriting == 1) {
        MoveFirst();
    }
    //If reach the end of page
    if (bufferPage.GetFirst(&fetchme) == 0) {
        pageIndex++;
        //If reach the end of file
        if (pageIndex >= diskFile.GetLength() - 1) {
            return 0;
        }
        //Else get next page
        bufferPage.EmptyItOut();
        diskFile.GetPage(&bufferPage, pageIndex);
        bufferPage.GetFirst(&fetchme);
    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    //If not reach the end of file
    while (GetNext(fetchme) == 1) {
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}
