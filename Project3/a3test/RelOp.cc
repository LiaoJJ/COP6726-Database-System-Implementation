#include "RelOp.h"
#include <iostream>
#include "BigQ.h"

//========================================SelectFile
typedef struct {
    DBFile &inFile;
    Pipe &outPipe;
    CNF &selOp;
    Record &literal;
} WorkerArg1;
void* workerMain1(void* arg){
    WorkerArg1* workerArg = (WorkerArg1*) arg;
    Record rec;
    while(workerArg->inFile.GetNext(rec, workerArg->selOp, workerArg->literal)){
        workerArg->outPipe.Insert(&rec);
    }
    workerArg->outPipe.ShutDown();
    return NULL;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    WorkerArg1* workerArg = new WorkerArg1{inFile, outPipe, selOp, literal};
    pthread_create(&worker, NULL, workerMain1, (void*) workerArg);
}

void SelectFile::WaitUntilDone () {
	pthread_join (worker, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}


//========================================SelectPipe
typedef struct {
    Pipe &inPipe;
    Pipe &outPipe;
    CNF &selOp;
    Record &literal;
} WorkerArg2;
void* workerMain2(void*arg){
    ComparisonEngine comp;
    WorkerArg2* workerArg = (WorkerArg2*) arg;
    Record rec;
    while(workerArg->inPipe.Remove(&rec)){
        if(comp.Compare(&rec, &workerArg->literal, &workerArg->selOp)){
            workerArg->outPipe.Insert(&rec);
        }
    }
    workerArg->outPipe.ShutDown();
    return NULL;
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    WorkerArg2* workerArg = new WorkerArg2{inPipe, outPipe, selOp, literal};
    pthread_create(&worker, NULL, workerMain2, (void*) workerArg);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (worker, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {

}



//========================================Sum
typedef struct {
    Pipe &inPipe;
    Pipe &outPipe;
    Function &computeMe;
} WorkerArg3;
void* workerMain3(void*arg){
    int intSum = 0, intResult = 0;
    double doubleSum = 0.0, doubleResult = 0.0;
    ComparisonEngine comp;
    WorkerArg3* workerArg = (WorkerArg3*) arg;
    Record rec;
    Type t;
    while(workerArg->inPipe.Remove(&rec)){
        t = workerArg->computeMe.Apply(rec, intResult, doubleResult);
        if(t==Int){
            intSum += intResult;
        }
        else{
            doubleSum += doubleResult;
        }
    }

    Attribute DA = {"SUM", t};
    Schema out_sch ("out_sch", 1, &DA);
    Record res;
    char charsRes[100];
    if(t==Int){
        sprintf(charsRes, "%d|", intSum);
    }
    else{
        sprintf(charsRes, "%lf|", doubleSum);
    }
    res.ComposeRecord(&out_sch, charsRes);
    workerArg->outPipe.Insert(&res);
    workerArg->outPipe.ShutDown();
    return NULL;
}
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
    WorkerArg3* workerArg = new WorkerArg3{inPipe, outPipe, computeMe};
    pthread_create(&worker, NULL, workerMain3, (void*) workerArg);
}
void Sum::WaitUntilDone (){
    pthread_join(worker, NULL);
}
void Sum::Use_n_Pages (int n){

}







//========================================GroupBy
typedef struct {
    Pipe &inPipe;
    Pipe &outPipe;
    OrderMaker &groupAtts;
    Function &computeMe;
    int use_n_pages;
} WorkerArg4;
void* workerMain4(void*arg){
    WorkerArg4* workerArg = (WorkerArg4*) arg;
    Pipe sorted(100);
    BigQ bigQ(workerArg->inPipe, sorted, workerArg->groupAtts, workerArg->use_n_pages);
    int intRes = 0, intSum = 0;
    double doubleRes = 0.0, doubleSum = 0.0;
    ComparisonEngine cmp;
    Record prev;
    Record cur;
    Type t;
    Attribute DA = {"SUM", t};
    Schema out_sch ("out_sch", 1, &DA);
    bool firstTime = true;
    while(sorted.Remove(&cur)){
        if(!firstTime && cmp.Compare(&cur, &prev, &workerArg->groupAtts)!=0){
            // different, this is a new group
//            cur.Print(&workerArg->groupAtts);
//            prev.Print(&workerArg->groupAtts);
            cout<<"==="<<endl;
            Record res;
            char charsRes[100];
            if(t==Int){
                sprintf(charsRes, "%d|", intSum);
            }
            else {
                sprintf(charsRes, "%lf|", doubleSum);
            }
            res.ComposeRecord(&out_sch, charsRes);
            workerArg->outPipe.Insert(&res);

            intSum = 0;
            doubleSum = 0.0;
        }
        // add to the previous group
        firstTime = false;
        t = workerArg->computeMe.Apply(cur, intRes, doubleRes);
        if(t==Int){
            intSum += intRes;
        }
        else {
            doubleSum += doubleRes;
        }
        prev.Copy(&cur);
    }
    // for the last group
    Record res;
    char charsRes[100];
    if(t==Int){
        sprintf(charsRes, "%d|", intSum);
    }
    else {
        sprintf(charsRes, "%lf|", doubleSum);
    }
    res.ComposeRecord(&out_sch, charsRes);
    workerArg->outPipe.Insert(&res);
    workerArg->outPipe.ShutDown();
    return NULL;
}
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
    WorkerArg4* workerArg = new WorkerArg4{inPipe, outPipe, groupAtts, computeMe, use_n_pages};
    pthread_create(&worker, NULL, workerMain4, (void*) workerArg);
}
void GroupBy::WaitUntilDone (){
    pthread_join(worker, NULL);
}
void GroupBy::Use_n_Pages (int n){
    use_n_pages = n;
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    //Construct parameters and send them to main worker
    ProjectArg* projectArg = new ProjectArg;
    projectArg->inPipe = &inPipe;
    projectArg->outPipe = &outPipe;
    projectArg->keepMe = keepMe;
    projectArg->numAttsInput = numAttsInput;
    projectArg->numAttsOutput = numAttsOutput;
    pthread_create(&workerThread, NULL, ProjectWorker, (void*) projectArg);
}

void* ProjectWorker (void* arg) {
    ProjectArg* projectArg = (ProjectArg*) arg;
    Record record;
    //Suck records from input pipe and do the project, then put them into output pipe
    while (projectArg->inPipe->Remove(&record) == 1) {
        Record* tempRecord = new Record;
        tempRecord->Consume(&record);
        tempRecord->Project(projectArg->keepMe, projectArg->numAttsOutput, projectArg->numAttsInput);
        projectArg->outPipe->Insert(tempRecord);     
    }
    projectArg->outPipe->ShutDown();
    return NULL;
}

void Project::WaitUntilDone() {
    pthread_join(workerThread, NULL);
}

void Project::Use_n_Pages(int n) {

}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 
    //Construct parameters and send them to main worker
    DuplicateRemovalArg* duplicateRemovalArg = new DuplicateRemovalArg;
    duplicateRemovalArg->inPipe = &inPipe;
    duplicateRemovalArg->outPipe = &outPipe;
    OrderMaker* order = new OrderMaker(&mySchema);
    duplicateRemovalArg->order = order;
    if (this->runLen <= 0)
        duplicateRemovalArg->runLen = 8;
    else
        duplicateRemovalArg->runLen = this->runLen;
    pthread_create(&workerThread, NULL, DuplicateRemovalWorker, (void*) duplicateRemovalArg);
}

void* DuplicateRemovalWorker (void* arg) {
    DuplicateRemovalArg* duplicateRemovalArg = (DuplicateRemovalArg*) arg;
    ComparisonEngine comparisonEngine;
    Record cur, last;
    Pipe* sortedPipe = new Pipe(1000);
    //Using BigQ to sort the records and put sorted records into a new pipe
    BigQ* bq = new BigQ(*(duplicateRemovalArg->inPipe), *sortedPipe, *(duplicateRemovalArg->order), duplicateRemovalArg->runLen);
    sortedPipe->Remove(&last);
    Schema schema ("catalog", "partsupp");
    //Check duplicate records by using sorted pipe, only forward distinct records to output pipe
    while (sortedPipe->Remove(&cur) == 1) {
        if (comparisonEngine.Compare(&last, &cur, duplicateRemovalArg->order) != 0) {
            Record* tempRecord = new Record;
            tempRecord->Consume(&last);
            duplicateRemovalArg->outPipe->Insert(tempRecord);
            last.Consume(&cur);
        }
    }
    duplicateRemovalArg->outPipe->Insert(&last);
    duplicateRemovalArg->outPipe->ShutDown();
    return NULL;
}

void DuplicateRemoval::WaitUntilDone () { 
    pthread_join(workerThread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n) {
    this->runLen = n;
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    //Construct parameters and send them to main worker
    WriteOutArg* writeOutArg = new WriteOutArg;
    writeOutArg->inPipe = &inPipe;
    writeOutArg->outFile = outFile;
    writeOutArg->schema = &mySchema;
    pthread_create(&workerThread, NULL, WriteOutWorker, (void*) writeOutArg);
}

void* WriteOutWorker (void* arg) {
    //Write records into file according to format defined in Print() method of Record
    WriteOutArg* writeOutArg = (WriteOutArg*) arg;
    Record cur;
    while (writeOutArg->inPipe->Remove(&cur) == 1) {
        int numOfAtts = writeOutArg->schema->GetNumAtts();
        Attribute *attribute = writeOutArg->schema->GetAtts();
        for (int i = 0; i < numOfAtts; i++) {
            fprintf(writeOutArg->outFile, "%s:", attribute[i].name);
            int pointer = ((int *) cur.bits)[i + 1];
            fprintf(writeOutArg->outFile, "[");
            if (attribute[i].myType == Int) {
                int *writeOutInt = (int*) &(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%d", *writeOutInt);
            }
            else if (attribute[i].myType == Double) {
                double *writeOutDouble = (double*) &(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%f", *writeOutDouble);
            }
            else if (attribute[i].myType == String) {
                char* writeOutString = (char*) &(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%s", writeOutString);
            }
            fprintf(writeOutArg->outFile, "]");
            if (i != numOfAtts - 1)
                fprintf(writeOutArg->outFile, ", ");
        }
        fprintf(writeOutArg->outFile, "\n");
    }
    return NULL;
}

void WriteOut::WaitUntilDone () {
    pthread_join(workerThread, NULL);
}

void WriteOut::Use_n_Pages (int n) { 

}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    //Construct parameters and send them to main worker
    JoinArg* joinArg = new JoinArg;
    joinArg->inPipeL = &inPipeL;
    joinArg->inPipeR = &inPipeR;
    joinArg->outPipe = &outPipe;
    joinArg->selOp = &selOp;
    joinArg->literal = &literal;
    if (this->runLen <= 0)
        joinArg->runLen = 8;
    else
        joinArg->runLen = this->runLen;
    pthread_create(&workerThread, NULL, JoinWorker, (void*) joinArg);
}

void* JoinWorker(void* arg) {
    JoinArg* joinArg = (JoinArg*) arg;
    OrderMaker leftOrder, rightOrder;
    joinArg->selOp->GetSortOrders(leftOrder, rightOrder);
    //Decide to sort join merge or blocknested join
    if (leftOrder.numAtts > 0 && rightOrder.numAtts > 0) {
        cout << "Enter sort merge " << endl;
        JoinWorker_Merge(joinArg, &leftOrder, & rightOrder);
    }
    else {
        cout << "BlockNestJoin" << endl;
        JoinWorker_BlockNested(joinArg);
    }
    joinArg->outPipe->ShutDown();
    return NULL;
}

//This method is used to merge two records into single record
void JoinWorker_AddMergedRecord(Record* leftRecord, Record* rightRecord, Pipe* pipe) {
    int numOfAttsLeft = ((((int*) leftRecord->bits)[1]) / sizeof(int)) - 1;
    int numOfAttsRight = ((((int*) rightRecord->bits)[1]) / sizeof(int)) - 1;
    int* attsToKeep = new int[numOfAttsLeft + numOfAttsRight];
    for (int i = 0; i < numOfAttsLeft; i++)
        attsToKeep[i] = i;
    for (int i = numOfAttsLeft; i < numOfAttsLeft + numOfAttsRight; i++)
        attsToKeep[i] = i - numOfAttsLeft;
    Record mergedRecord;
    mergedRecord.MergeRecords(leftRecord, rightRecord, numOfAttsLeft, numOfAttsRight, attsToKeep, numOfAttsLeft + numOfAttsRight, numOfAttsLeft);
    pipe->Insert(&mergedRecord);
}

//Sort merge Join
void JoinWorker_Merge(JoinArg* joinArg, OrderMaker* leftOrder, OrderMaker* rightOrder) {
    //First using BigQ to sort given records in two pipes
    Pipe* sortedLeftPipe = new Pipe(1000);
    Pipe* sortedRightPipe = new Pipe(1000);
    BigQ* tempL = new BigQ(*(joinArg->inPipeL), *sortedLeftPipe, *leftOrder, joinArg->runLen);
    BigQ* tempR = new BigQ(*(joinArg->inPipeR), *sortedRightPipe, *rightOrder, joinArg->runLen);
    // cout << "BigQ created" << endl;
    Record leftRecord;
    Record rightRecord;
    bool isFinish = false;
    if (sortedLeftPipe->Remove(&leftRecord) == 0)
        isFinish = true;
    if (sortedRightPipe->Remove(&rightRecord) == 0)
        isFinish = true;
    // cout << "BigQ outputed" << endl;
    ComparisonEngine comparisonEngine;
    //Then do the merge part, merge same record together to join
    while (!isFinish) {
        int compareRes = comparisonEngine.Compare(&leftRecord, leftOrder, &rightRecord, rightOrder);
        //If left record equal to right record, we merge them together and insert it into output pipe
        if (compareRes == 0) {
            vector<Record*> vl;
            vector<Record*> vr;
            //Find all idential and continuous records in the left pipe and put them into vector
            while (true) {
                Record* oldLeftRecord = new Record;
                oldLeftRecord->Consume(&leftRecord);
                vl.push_back(oldLeftRecord);
                if (sortedLeftPipe->Remove(&leftRecord) == 0) {
                    isFinish = true;
                    break;
                }
                if (comparisonEngine.Compare(&leftRecord, oldLeftRecord, leftOrder) != 0) {
                    break;
                }
            }
            //Find all idential and continuous records in the right pipe and put them into vector
            while (true) {
                Record* oldRightRecord = new Record;
                oldRightRecord->Consume(&rightRecord);
                // oldRightRecord->Print(&partsupp);
                vr.push_back(oldRightRecord);
                if (sortedRightPipe->Remove(&rightRecord) == 0) {
                    isFinish = true;
                    break;
                }
                if (comparisonEngine.Compare(&rightRecord, oldRightRecord, rightOrder) != 0) {
                    break;
                }
            }
            //Merge every part of them, and join
            for (int i = 0; i < vl.size(); i++) {
                for (int j = 0; j < vr.size(); j++) {
                    JoinWorker_AddMergedRecord(vl[i], vr[j], joinArg->outPipe);
                }
            }
            vl.clear();
            vr.clear();
        }
        //If left reocrd are larger, then we move right record
        else if (compareRes > 0) {
            if (sortedRightPipe->Remove(&rightRecord) == 0)
                isFinish = true;
        }
        //If right record are larger, then we move left record
        else {
            if (sortedLeftPipe->Remove(&leftRecord) == 0)
                isFinish = true;
        }
    }
    cout << "Finish read fron sorted pipe" << endl;
    while (sortedLeftPipe->Remove(&leftRecord) == 1);
    while (sortedRightPipe->Remove(&rightRecord) == 1);
}

//This is block nested join on default
void JoinWorker_BlockNested(JoinArg* joinArg) {
    //Literlly join every pair of records.
    DBFile tempFile;
    char* fileName = new char[100];
    sprintf(fileName, "BlockNestedTemp%d.bin", pthread_self());
    tempFile.Create(fileName, heap, NULL);
    tempFile.Open(fileName);
    Record record;
    while (joinArg->inPipeL->Remove(&record) == 1)
        tempFile.Add(record);
    
    Record record1, record2;
    ComparisonEngine comparisonEngine;
    while (joinArg->inPipeR->Remove(&record1) == 1) {
        tempFile.MoveFirst();
        while (tempFile.GetNext(record) == 1) {
            if (comparisonEngine.Compare(&record1, &record2, joinArg->literal, joinArg->selOp)) {
                JoinWorker_AddMergedRecord(&record1, &record2, joinArg->outPipe);
            }
        }
    }
}

void Join::WaitUntilDone () { 
    pthread_join(workerThread, NULL);
}
    
void Join::Use_n_Pages (int n) { 
    this->runLen = n;
}
