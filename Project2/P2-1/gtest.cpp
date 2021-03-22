#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "BigQ.h"
#include "BigQ.cc"
#include <stdlib.h>
using namespace std;

class BigQTest : public ::testing::Test {
protected:
    BigQTest() {
        // You can do set-up work for each test here.
    }

    ~BigQTest() override {
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
    // File file;
    char* regionFileName = "localTest/region.bin";
    char* lineitemFileName = "localTest/lineitem.bin";
    char* testFileName = "localTest/test.bin";
};

TEST_F(BigQTest, UpdateTopRecordForRunTest) {
    // RecordComparer* comparer = new RecordComparer(new OrderMaker);
    File file;
    file.Open(1, regionFileName);
    class Run* run = new class Run(&file, 0, 1);
    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record tempRecord;
    bufferPage.GetFirst(&tempRecord);
    while (bufferPage.GetFirst(&tempRecord) == 1) {
        EXPECT_EQ(run->UpdateTopRecord(), 1);
    }
    EXPECT_EQ(run->UpdateTopRecord(), 0);
    file.Close();
}

TEST_F(BigQTest, RecordComparerTest) {
    File file;
    file.Open(1, regionFileName);
    //Initiate the proority queue for recard comparer
    Schema* scheme = new Schema("catalog", "region");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<Record*, vector<Record*>, RecordComparer> recordQueue (order);
    ComparisonEngine comparisonEngine;

    //Puting records into comparer based priority queue
    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record* readindRecord = new Record;
    while (bufferPage.GetFirst(readindRecord)) {
        recordQueue.push(readindRecord);
        readindRecord = new Record;
    }

    bufferPage.EmptyItOut();
    file.GetPage(&bufferPage, 0);
    Record rec[2];
    Record *last = NULL, *prev = NULL;
    int i = 0;
    while (bufferPage.GetFirst(&rec[i%2]) == 1) {
        prev = last;
        last = &rec[i%2];
        if (prev && last) {
            EXPECT_EQ(comparisonEngine.Compare(prev, last, order), -1);
        }
        i++;
    }
    file.Close();
}

TEST_F(BigQTest, RunComparerTest) {
    //Initiate the proority queue for recard comparer
    File file;
    file.Open(1, lineitemFileName);
    Schema* scheme = new Schema("catalog", "lineitem");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<class Run*, vector<class Run*>, RunComparer> runQueue (order);
    ComparisonEngine comparisonEngine;

    class Run* run1 = new class Run(&file, 0, 1);
    class Run* run2 = new class Run(&file, 1, 1);
    runQueue.push(run1);
    runQueue.push(run2);

    //Take top record from two different run
    Record one, two;
    one.Copy(runQueue.top()->topRecord);
    runQueue.pop();
    two.Copy(runQueue.top()->topRecord);
    runQueue.pop();
    EXPECT_EQ(comparisonEngine.Compare(&one, &two, order), -1);

    file.Close();
}

TEST_F(BigQTest, recordToRunTest) {
    //Reading records from file to queue
    File file;
    file.Open(1, regionFileName);
    Schema* scheme = new Schema("catalog", "region");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<Record*, vector<Record*>, RecordComparer> recordQueue (order);
    ComparisonEngine comparisonEngine;
    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record* readindRecord = new Record;
    while (bufferPage.GetFirst(readindRecord)) {
        recordQueue.push(readindRecord);
        readindRecord = new Record;
    }

    //Writing records from queue to run
    File testFile;
    testFile.Open(0, testFileName);
    Page testPage;
    //Initiate the proority queue for recard comparer
    int pageIndex = 0;
    priority_queue<class Run*, vector<class Run*>, RunComparer> runQueue (order);
    recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
    EXPECT_EQ(recordQueue.size(), 0);
    EXPECT_EQ(runQueue.size(), 1);
    file.Close();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
