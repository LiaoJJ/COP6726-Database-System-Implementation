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
#include "RelOp.cc"
#include "RelOp.h"
#include "Function.h"
#include <stdlib.h>
#include "Defs.h"
using namespace std;

class RefOpTest : public ::testing::Test {

protected:
    RefOpTest() {
        // You can do set-up work for each test here.
    }

    ~RefOpTest() override {
        // You can do clean-up work that doesn't throw exceptions here.
    }
    void SetUp() override {
        // Code here will be called immediately after the constructor (right
        // before each test).
        // file.Open(1, "localTest/region.bin");
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
        // file.Close();
    }
};

TEST_F(RefOpTest, ConstructorTest) {
    SelectFile sf;
}

TEST_F(RefOpTest, WaitUntilDoneTest) {
    SelectFile sf;
    sf.WaitUntilDone();
}


TEST_F(RefOpTest, Use_n_PagesTest) {
    GroupBy* sf = new GroupBy();
    int VALUE = 100;
    sf->Use_n_Pages(VALUE);
    EXPECT_EQ(VALUE, sf->use_n_pages);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
