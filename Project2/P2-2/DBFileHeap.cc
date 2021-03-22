//
// Created by LJJ on 2/24/21.
//

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileHeap.h"
#include "Defs.h"
#include <iostream>

// stub file .. replace it with your own DBFileHeap.cc

DBFileHeap::DBFileHeap () {
    isFileOpen = 0;
    isWriting = 0;
    pageIndex = 0;
}

int DBFileHeap::Create (char *f_path, fType f_type, void *startup) {
    if (isFileOpen == 1) {
        cerr << "Cannot recreate file since file already opened!" << "\n";
        return 0;
    }
    diskFile.Open(0, const_cast<char *>(f_path));
    pageIndex = 0;
    isWriting = 0;
    isFileOpen = 1;
    MoveFirst();
    return 1;
}

void DBFileHeap::Load (Schema &f_schema, char *loadpath) {
    if (isFileOpen == 0) {
        cerr << "Cannot loading while file not open!";
        return;
    }
    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    ComparisonEngine comp;

    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
        this->Add(temp);
    }
    if (isWriting == 1)
        diskFile.AddPage(&bufferPage, pageIndex);
}

int DBFileHeap::Open (char *f_path) {
    if (isFileOpen == 1) {
        cerr << "File already opened!" << "\n";
        return 0;
    }
    diskFile.Open(1, const_cast<char *>(f_path));
    pageIndex = 0;
    //Reading mode on default
    isWriting = 0;
    isFileOpen = 1;
    MoveFirst();
    return 1;
}

void DBFileHeap::MoveFirst () {
    if (isFileOpen == 0) {
        cerr << "Cannot MoveFirst while file not opening!" << "\n";
        return;
    }
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
}

int DBFileHeap::Close () {
    if (isFileOpen == 0) {
        cerr << "File is not opened!" << "\n";
        return 0;
    }
    if (isWriting == 1)
        diskFile.AddPage(&bufferPage, pageIndex);
    bufferPage.EmptyItOut();
    diskFile.Close();
    isFileOpen = 0;
    // cout << "Closing file, length of file is " << diskFile.GetLength() << "Pages" << "\n";
    return 1;
}

void DBFileHeap::Add (Record &rec) {
    if (isFileOpen == 0) {
        cerr << "Cannot writing while file not opening!" << "\n";
        return;
    }
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

int DBFileHeap::GetNext (Record &fetchme) {
    if (isFileOpen == 0) {
        cerr << "Cannot reading while file not opening!" << "\n";
        return 0;
    }
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

int DBFileHeap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    //If not reach the end of file
    while (GetNext(fetchme) == 1) {
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}
