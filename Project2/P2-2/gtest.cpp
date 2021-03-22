#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "DBFileHeap.cc"
#include "DBFileSorted.cc"
#include "DBFileSorted.h"
#include "DBFileTree.h"
#include "DBFileTree.cc"
#include "BigQ.h"
#include "BigQ.cc"
#include <stdlib.h>
#include "Defs.h"
using namespace std;

class DBFileTest : public ::testing::Test {
protected:
    DBFileTest() {
        // You can do set-up work for each test here.
    }

    ~DBFileTest() override {
        // You can do clean-up work that doesn't throw exceptions here.
    }
    void SetUp() override {
        // Code here will be called immediately after the constructor (right
        // before each test).
        // file.Open(1, "localTest/region.bin");
        dbfile = new DBFile();
        orderMaker.numAtts = 1;
        orderMaker.whichAtts[0] = 4;
        orderMaker.whichTypes[0] = String;
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
        // file.Close();
        delete dbfile;
    }
    DBFile* dbfile;
    OrderMaker orderMaker;
};

TEST_F(DBFileTest, CreateTest) {
    struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
    int aa = dbfile->Create("./gtest.bin", sorted, &startup);
    cout<<"=="<<aa<<endl;
//    EXPECT_EQ(dbfile->Create("./gtest.bin", sorted, &startup), 1);
    dbfile->Close();
}

TEST_F(DBFileTest, OpenTest) {
    EXPECT_EQ(dbfile->Open("./gtest.bin"), 1);
    dbfile->Close();
}

TEST_F(DBFileTest, CloseTest) {
    dbfile->Open("./gtest.bin");
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST_F(DBFileTest, OpenAndCloseTest) {
    dbfile->Open("./gtest.bin");
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST_F(DBFileTest, CreateAndCloseTest) {
    struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
    dbfile->Create("./gtest.bin", sorted, &startup);
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST_F(DBFileTest, GetNextTest) {
struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
dbfile->Open("./gtest.bin");
Record rec;
EXPECT_EQ(dbfile->GetNext(rec), 0);
dbfile->Close();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
