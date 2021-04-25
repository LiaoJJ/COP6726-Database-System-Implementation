#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <unordered_map>
#include <string>
#include <fstream>
#include <vector>
#include <set>
#include <string>
#include <map>
#include <sstream>

using namespace std;

class RelationInfo{

    
public:
    unordered_map<string, int> attributeMap;
    int numOfTuple;
    string relationName;
    set<string> joinedRelation;
    RelationInfo(string relationName, int numOfTuple){
        this->numOfTuple = numOfTuple;
        this->relationName = relationName;
        joinedRelation.insert(relationName);
    }
    ~RelationInfo(){
        attributeMap.clear();
        joinedRelation.clear();
    }
};


class Statistics
{
private:

public:
	Statistics();
	Statistics(Statistics &copyMe);
	~Statistics();

    unordered_map<string, RelationInfo*> relationMap;
    bool helpPartitionAndParseTree(struct AndList *parseTree, char *subsetNames[], int numToJoin, unordered_map<string,long> &uniqvallist);
    bool helpAttributes(char *value,char *subsetNames[], int numToJoin,unordered_map<string,long> &uniqvallist);
    double helpTuplesEstimate(struct OrList *orList, unordered_map<string,long> &uniqvallist);
    void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

set<string> getJoinedRelations(string subsetName);

string serializationJoinedRelations(set<string> joinedRelation);




#endif
