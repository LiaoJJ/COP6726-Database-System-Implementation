#include "gtest/gtest.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include "NodeForQuery.h"


using namespace std;


class a5GTest : public ::testing::Test {

protected:
    a5GTest() {
        // You can do set-up work for each test here.
    }

    ~a5GTest() override {
        // You can do clean-up work that doesn't throw exceptions here.
    }
    void SetUp() override {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }
};

TEST_F(a5GTest, DBFileHeapTest){
    DBFile file;
    Schema schema("catalog", "nation");
    Record rec;
    file.Open("bin/nation.bin");
//    file.Create("bin/nation.bin", heap, NULL);
//    file.Load(schema, "tpch/nation.tbl");
    while(file.GetNext(rec)){
        rec.Print(&schema);
    }
//    file.Close();
}

TEST_F(a5GTest, ConstructorTest){
    NodeForQuery* node = new JoinNode(1, NULL, NULL);
    NodeForQuery* node2 = new SelectFileNode(1, NULL);
}

TEST_F(a5GTest, DestructorTest){
    NodeForQuery* node = new JoinNode(1, NULL, NULL);
    NodeForQuery* node2 = new SelectFileNode(1, NULL);
    delete node;
    delete node2;
}

TEST_F(a5GTest, makeChildTest) {
    NodeForQuery one, two;
    one.outputId = 1;
    two.outputId = 2;
    one.makeChild(&two, true);
    EXPECT_EQ(one.leftId, two.outputId);
    EXPECT_TRUE(one.leftNode->outputId == two.outputId);
    EXPECT_TRUE(two.parentNode->outputId == one.outputId);
}

TEST_F(a5GTest, printKeepMeTest) {
    vector<int> testForKeepMe;
    testForKeepMe.push_back(1);
    testForKeepMe.push_back(2);
    testForKeepMe.push_back(3);
    testForKeepMe.push_back(4);

    ProjectNode* node = new ProjectNode(1, NULL, testForKeepMe);
    string sKeepMe = node->getKeepMe();
    string correctAnswer = "(1, 2, 3, 4)";
    EXPECT_TRUE(sKeepMe == correctAnswer);
}

TEST_F(a5GTest, constructTest) {
    vector<int> testForKeepMe;
    testForKeepMe.push_back(1);
    testForKeepMe.push_back(2);
    testForKeepMe.push_back(3);
    testForKeepMe.push_back(4);
    ProjectNode* node = new ProjectNode(0, NULL, testForKeepMe);
    EXPECT_EQ(node->outputId, 0);
}



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
