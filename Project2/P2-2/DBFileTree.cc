//
// Created by LJJ on 2/24/21.
//

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileTree.h"
#include "Defs.h"

// stub file .. replace it with your own DBFileTree.cc

DBFileTree::DBFileTree () {

}

int DBFileTree::Create (char *f_path, fType f_type, void *startup) {
}

void DBFileTree::Load (Schema &f_schema, char *loadpath) {
}

int DBFileTree::Open (char *f_path) {
}

void DBFileTree::MoveFirst () {
}

int DBFileTree::Close () {
}

void DBFileTree::Add (Record &rec) {
}

int DBFileTree::GetNext (Record &fetchme) {
    return 0;
}

int DBFileTree::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
