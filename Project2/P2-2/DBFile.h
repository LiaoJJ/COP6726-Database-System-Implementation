#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileGeneric.h"

typedef enum {heap, sorted, tree} fType;
typedef struct {OrderMaker *myOrder; int runLength;} SortedInfo;
// stub DBFile header..replace it with your own DBFile.h 


class DBFileGeneric{
public:
    virtual int Create (char *fpath, fType f_type,  void *startup) = 0;
    virtual int Open (char *fpath) = 0;
    virtual int Close () = 0;

    virtual void Load (Schema &myschema, char *loadpath) = 0;

    virtual void MoveFirst () = 0;
    virtual void Add (Record &addme) = 0;
    virtual int GetNext (Record &fetchme) = 0;
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};


class DBFile {
private:
    DBFileGeneric* myInernalVar;

public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
