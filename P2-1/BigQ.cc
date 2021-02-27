#include "BigQ.h"

void* workerMain(void* arg) {
    WorkerArg* workerArg = (WorkerArg*) arg;
    priority_queue<Run*, vector<Run*>, RunComparer> runQueue(workerArg->order);
    priority_queue<Record*, vector<Record*>, RecordComparer> recordQueue (workerArg->order);
    vector<Record* > recBuff;
    Record curRecord;

    //Set disk based file for sorting
    File file;
    char* fileName = "tmp.bin";
    file.Open(0, fileName);

    //Buffer page used for disk based file
    Page bufferPage;
    int pageIndex = 0;
    int pageCounter = 0;
    //Retrieve all records from input pipe
    while (workerArg->in->Remove(&curRecord) == 1) {
        Record* tmpRecord = new Record;
        tmpRecord->Copy(&curRecord);
        //Add to another page if current page is full
        if (bufferPage.Append(&curRecord) == 0) {
            pageCounter++;
            bufferPage.EmptyItOut();

            //Add to another run if current run is full
            if (pageCounter == workerArg->runlen) {
                recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
                recordQueue = priority_queue<Record*, vector<Record*>, RecordComparer> (workerArg->order);
                pageCounter = 0;
            }

            bufferPage.Append(&curRecord);
        }

        recordQueue.push(tmpRecord);
    }
    // Handle the last run
    if (!recordQueue.empty()) {
        recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
        recordQueue = priority_queue<Record*, vector<Record*>, RecordComparer> (workerArg->order);
    }
    // Merge for all runs
    while (!runQueue.empty()) {
        Run* run = runQueue.top();
        runQueue.pop();
        workerArg->out->Insert(run->topRecord);
        if (run->UpdateTopRecord() == 1) {
            runQueue.push(run);
        }
    }
    file.Close();
    workerArg->out->ShutDown();
    return NULL;
}

//Used for puting records into a run, which is disk file based
void* recordQueueToRun(priority_queue<Record*, vector<Record*>, RecordComparer>& recordQueue, 
    priority_queue<Run*, vector<Run*>, RunComparer>& runQueue, File& file, Page& bufferPage, int& pageIndex) {

    bufferPage.EmptyItOut();
    int startIndex = pageIndex;
    while (!recordQueue.empty()) {
        Record* tmpRecord = new Record;
        tmpRecord->Copy(recordQueue.top());
        recordQueue.pop();
        if (bufferPage.Append(tmpRecord) == 0) {
            file.AddPage(&bufferPage, pageIndex++);
            bufferPage.EmptyItOut();
            bufferPage.Append(tmpRecord);
        }
    }
    file.AddPage(&bufferPage, pageIndex++);
    bufferPage.EmptyItOut();
    Run* run = new Run(&file, startIndex, pageIndex - startIndex);
    runQueue.push(run);
    return NULL;
}



BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    pthread_t worker;
    //Construct arguement used for worker thread
    WorkerArg* workerArg = new WorkerArg;
    workerArg->in = &in;
    workerArg->out = &out;
    workerArg->order = &sortorder;
    workerArg->runlen = runlen;
    pthread_create(&worker, NULL, workerMain, (void*) workerArg);
    pthread_join(worker, NULL);
	out.ShutDown ();
}

BigQ::~BigQ () {

}

Run::Run(File* file, int start, int length) {
    fileBase = file;
    startPage = start;
    runLength = length;
    curPage = start;
    fileBase->GetPage(&bufferPage, startPage);
    topRecord = new Record;
    UpdateTopRecord();
}

int Run::UpdateTopRecord() {
    //if bufferPage is full
    if (bufferPage.GetFirst(topRecord) == 0) {
        //if reach the last page
        curPage++;
        if (curPage == startPage + runLength) {
            return 0;
        }
        bufferPage.EmptyItOut();
        fileBase->GetPage(&bufferPage, curPage);
        bufferPage.GetFirst(topRecord);
    }
    return 1;
}

RecordComparer::RecordComparer(OrderMaker* orderMaker) {
    order = orderMaker;
}

bool RecordComparer::operator () (Record* left, Record* right) {
    ComparisonEngine comparisonEngine;
    if (comparisonEngine.Compare(left, right, order) >= 0)
        return true;
    return false;
}

RunComparer::RunComparer(OrderMaker* orderMaker) {
    order = orderMaker;
}

bool RunComparer::operator () (Run* left, Run* right) {
    ComparisonEngine comparisonEngine;
    if (comparisonEngine.Compare(left->topRecord, right->topRecord, order) >= 0)
        return true;
    return false;
}


