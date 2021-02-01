
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include <stdlib.h>
using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {

    // try to parse the CNF
    cout << "Enter in your CNF: ";
    if (yyparse() != 0) {
        cout << "Can't parse your CNF.\n";
        exit (1);
    }

    // suck up the schema from the file
    Schema lineitem ("catalog", "lineitem");

    // grow the CNF expression from the parse tree
    CNF myComparison;
    Record literal;
    myComparison.GrowFromParseTree (final, &lineitem, literal);

    // print out the comparison to the screen
    myComparison.Print ();

    // now open up the text file and start procesing it
    FILE *tableFile = fopen ("../../tpch-dbgen/10M/lineitem.tbl", "r");

    Record temp;
    Schema mySchema ("catalog", "lineitem");

    //char *bits = literal.GetBits ();
    //cout << " numbytes in rec " << ((int *) bits)[0] << endl;
    //literal.Print (&supplier);

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    //Write data into File
    DBFile dbf;
//    dbf.Create("lineitem.bin", heap, NULL);
    dbf.Open("lineitem.bin");
//    File file;
//    file.Open(0, "test111.txt");
//    off_t writeIndex = 0;
//    Page page;
    int counter = 0, validCounter = 0;
    ComparisonEngine comp;
    while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
        counter++;
        if (counter % 10000 == 0) {
            cerr << counter << "\n";
        }

        if (comp.Compare (&temp, &literal, &myComparison)) {
            temp.Print(&mySchema);
            dbf.Add(temp);
            validCounter++;
        }
        // dbf.MoveFirst();
        // dbf.Add(temp);
        if (validCounter >= 3) {
            cout << "Start temp reading" << "\n";
            while (dbf.GetNext(temp, myComparison, literal) == 1) {
                temp.Print(&mySchema);
            }
            validCounter = 0;
            cout << "Finish temp reading" << "\n";
        }
    }

//    dbf.Close();
    cout << "Done writing, start reading" << "\n";
    cout << "Done writing, start reading" << "\n";
    cout << "Done writing, start reading" << "\n";

//    dbf.Open(NULL);
    dbf.MoveFirst();
    while (dbf.GetNext(temp, myComparison, literal) == 1) {
        temp.Print(&mySchema);
    }
//    dbf.Close();
    dbf.MoveFirst();
    cout << "Second traverse" << "\n";
    cout << "Second traverse" << "\n";
    cout << "Second traverse" << "\n";

//    dbf.Open(NULL);
    while (dbf.GetNext(temp, myComparison, literal) == 1) {
        temp.Print(&mySchema);
    }
    dbf.Close();

    dbf.Open("lineitem.bin");
    cout << "Third traverse" << "\n";
    cout << "Third traverse" << "\n";
    cout << "Third traverse" << "\n";

//    dbf.Open(NULL);
    while (dbf.GetNext(temp, myComparison, literal) == 1) {
        temp.Print(&mySchema);
    }
    dbf.Close();




//    File file;
//    Page page;
//    file.Open(1, "test111.txt");
//    for (int i = 0; i < file.GetLength() - 1; i++) {
//        file.GetPage(&page, i);
//        cout << "Start page " << i << "\n";
//        while (1) {
//            Record retrive;
//            if (page.GetFirst(&retrive) == 0)
//                break;
//            retrive.Print(&mySchema);
//            cout << "This is page " << i << "\n";
//        }
//    }




    //Retrieve all records from page
//    while (1) {
//        Record retrive;
//        if (page.GetFirst(&retrive) == 0)
//            break;
//        retrive.Print(&mySchema);
//    }

}


