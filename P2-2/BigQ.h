#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

};

//Class run represent run used for merging
class Run {

public:
	Run(File* file, int startPage, int runLength);
	//Used to update the top record of current run
	int UpdateTopRecord();
	//top record of current Run
	Record *topRecord; 

private: 
	File* fileBase;
	Page bufferPage;
	int startPage;
	int runLength;
	int curPage;
};

//Class used for comparing records for given CNF
class RecordComparer {

public:
	bool operator () (Record* left, Record* right);
	RecordComparer(OrderMaker *order);

private:
	OrderMaker *order;

};

//Class used for comparing run for given CNF
class RunComparer {

public:
	bool operator () (Run* left, Run* right);
	RunComparer(OrderMaker *order);

private:
	OrderMaker *order;

};

//Struct used as arguement for worker thread
typedef struct {
	
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
	
} WorkerArg;

//Main method executed by worker, worker will retrieve records from input pipe, 
//sort records into runs and puting all runs into priority queue, and geting sorted reecords
//from priority queue to output pipe
void* workerMain(void* arg);

//Used for take sequences of pages of records, and construct a run to hold such records, and put run
//into priority queue
void* recordQueueToRun(priority_queue<Record*, vector<Record*>, RecordComparer>& recordQueue, 
    priority_queue<Run*, vector<Run*>, RunComparer>& runQueue, File& file, Page& bufferPage, int& pageIndex);

#endif
