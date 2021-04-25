#include "Statistics.h"
#include <string>
#include <string.h>

Statistics::Statistics(Statistics &copyMe)
{   
    for (unordered_map<string,RelationInfo*>::iterator iter = relationMap.begin(); iter != relationMap.end(); iter++) {
        RelationInfo* curRelation = iter->second;
        //Copy number of tuple of relation
        RelationInfo* newRelation = new RelationInfo(curRelation->relationName, curRelation->numOfTuple);
        //Copy attribute map of realtion 
        for (unordered_map<string, int>::iterator kvPair = curRelation->attributeMap.begin(); kvPair != curRelation->attributeMap.end(); kvPair++) {
            newRelation->attributeMap[kvPair->first] = kvPair->second;
        }
        //Copy joined relations
        for (string jr : curRelation->joinedRelation) {
            if (jr.compare(curRelation->relationName) != 0)
                newRelation->joinedRelation.insert(jr);
        }
        copyMe.relationMap[newRelation->relationName] = newRelation;
    }
}

Statistics::Statistics()
{
}

Statistics::~Statistics()
{
}




void Statistics::AddRel(char *relName, int numTuples)
{
    string sName = string(relName);
    relationMap[sName] = new RelationInfo(sName, numTuples);
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string relationName = string(relName);
    string attributeName = string(attName);
    //If realtion does not exist
    if (relationMap.count(relationName) == 0) {
        return;
    }
    //Modify numDistincts if its value is -1
    if (numDistincts == -1) {
        numDistincts = relationMap[relationName]->numOfTuple;
    }
    relationMap[relationName]->attributeMap[attributeName] = numDistincts;

}

void Statistics::CopyRel(char *oldRelName, char *newRelName)
{
    string sOldRelationName = string(oldRelName);
    string sNewRelationName = string(newRelName);
    //If old realtion does not exist
    if (relationMap.count(sOldRelationName) == 0)
        return;
    RelationInfo* curRelation = relationMap[sOldRelationName];
    //Copy number of tuple of relation
    RelationInfo* newRelation = new RelationInfo(sNewRelationName, curRelation->numOfTuple);
    //Copy attribute map of realtion 
    for (unordered_map<string, int>::iterator kvPair = curRelation->attributeMap.begin(); kvPair != curRelation->attributeMap.end(); kvPair++) {
        newRelation->attributeMap[sNewRelationName + "." + kvPair->first] = kvPair->second;
    }
    //Copy joined relations
    for (string jr : curRelation->joinedRelation) {
        if (jr.compare(sOldRelationName) != 0)
            newRelation->joinedRelation.insert(jr);
    }
    relationMap[sNewRelationName] = newRelation;
}

void Statistics::Read(char *fromWhere)
{
    ifstream inputStream;
    inputStream.open(fromWhere);
    string word;

    RelationInfo* curRelation;
    while (inputStream >> word) {
        if (word.compare("Relation:") == 0) {
            inputStream >> word;
            string curRelationName = word;
            inputStream >> word;
            inputStream >> word;
            int numOfTuple = stoi(word);
            curRelation = new RelationInfo(curRelationName, numOfTuple);
        }
        else if (word.compare("JoinedRelation:") == 0) {
            inputStream >> word;
            curRelation->joinedRelation.insert(word);
        }
        else if (word.compare("Attribute:") == 0) {
            inputStream >> word;
            string curAttributeName = word;
            inputStream >> word;
            inputStream >> word;
            int numOfDistinct = stoi(word);
            if (numOfDistinct == -1)
                numOfDistinct = curRelation->numOfTuple;
            curRelation->attributeMap[curAttributeName] = numOfDistinct;
            
        }
        else if (word.compare("EndOfRelation") == 0) {
            relationMap[curRelation->relationName] = curRelation;
        }
    }
    inputStream.close();
}

void Statistics::Write(char *fromWhere)
{
    ofstream outputStream;
    outputStream.open(fromWhere);
    unordered_map<string, RelationInfo*>::iterator kvPair;
    for (kvPair = relationMap.begin(); kvPair != relationMap.end(); kvPair++) {
        //Write relation statistics info
        string curRelationName = kvPair->first;
        RelationInfo* curRelation = kvPair->second;
        int numOfTuple = curRelation->numOfTuple;
        outputStream << "Relation: " << curRelationName << " numOfTuple: " << numOfTuple << "\n"; 

        //Write joined relation information
        for (string rel : curRelation->joinedRelation) {
            outputStream << "JoinedRelation: " << rel << "\n";
        }

        unordered_map<string, int>* curAttributes = &(curRelation->attributeMap);
        unordered_map<string, int>::iterator kvPair2;
        for (kvPair2 = curAttributes->begin(); kvPair2 != curAttributes->end(); kvPair2++) {
            string curAttributeName = kvPair2->first;
            int numOfDistinct = kvPair2->second;
            outputStream << "Attribute: " << curAttributeName << " numOfDistinct: " << numOfDistinct << "\n";
        }
        outputStream << "EndOfRelation" << "\n";
    }
    outputStream.close();
}



double Statistics::Estimate(struct AndList *tree, char **relationNames, int numToJoin)
{
    double res;
    res = 1.0;
    unordered_map<string,long> uniqueList;
    if(helpPartitionAndParseTree(tree,relationNames,numToJoin,uniqueList))
    {
        string subsetName="G";
        unordered_map<string,long> tval;

        int subsetSize = numToJoin;
        int i=-1;
        while(++i<subsetSize){
            subsetName = subsetName + "," + relationNames[i];
        }
        i=-1;
        while(++i<numToJoin)
        {
            string serRes = serializationJoinedRelations(relationMap[relationNames[i]]->joinedRelation);
            tval[serRes] = relationMap[relationNames[i]]->numOfTuple;
        }

        res = 1000.0;
        while(tree!= nullptr){
            res= helpTuplesEstimate(tree->left,uniqueList) * res;
            tree=tree->rightAnd;
        }
        unordered_map<string,long>::iterator ti=tval.begin();
        while(ti!=tval.end())
        {
            res*=ti->second;
            ti++;
        }
    }
    else
    {
        return -1.0;
    }
    res = res/1000.0;
    return res;
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    double r = Estimate(parseTree, relNames, numToJoin);
    long numTuples =(long)round(r);
    string subsetName="G";
    int i=numToJoin;
    while(i-->0)
    {
        subsetName = subsetName + "," + string(relNames[i]);
    }
    i=numToJoin;
    while(--i>=0)
    {
        relationMap[relNames[i]]->joinedRelation = getJoinedRelations(subsetName);
        relationMap[relNames[i]]->numOfTuple = numTuples;
    }
}

set<string> getJoinedRelations(string subsetName) {
    set<string> joinedRelation;
    subsetName = subsetName + ",";
    int index = 0;
    int size = subsetName.size();
    while (subsetName.size() > 0) {
        index = subsetName.find(",");
        string sub = subsetName.substr(0, index);
        if (sub.compare("G") != 0) {
            joinedRelation.insert(sub);
        }
        subsetName.erase(0, index + 1);
    }
    return joinedRelation;
}

string serializationJoinedRelations(set<string> joinedRelation) {
    if (joinedRelation.size() == 1)
        return *joinedRelation.begin();
    string res = "G";
    for (string rel : joinedRelation)
        res = res + "," + rel;
    return res;
}

bool Statistics::helpPartitionAndParseTree(struct AndList *tree, char *relationNames[], int sizeOfAttributesJoin,unordered_map<string,long> &uniqueList)
{
    bool res;
    res = true;
    while(!(tree==NULL || !res)){
        struct OrList *orListTop;
        orListTop=tree->left;
        while(!(orListTop==NULL || !res))
        {
            struct ComparisonOp *cmpOp = orListTop->left;
            if(!(cmpOp->left->code!=NAME || cmpOp->code!=STRING || helpAttributes(cmpOp->left->value,relationNames,sizeOfAttributesJoin,uniqueList))) {
                cout<<"\n"<< cmpOp->left->value<<" Does Not Exist";
                res=false;
            }
            if(!( cmpOp->right->code!=NAME || cmpOp->code!=STRING || helpAttributes(cmpOp->right->value,relationNames,sizeOfAttributesJoin,uniqueList))) {
                res=false;
            }
            orListTop=orListTop->rightOr;
        }
        tree=tree->rightAnd;
    }
    if(false==res) return res;
    unordered_map<string,int> tbl;
    int i=0;
    while(i<sizeOfAttributesJoin){
        string gn = serializationJoinedRelations(relationMap[relationNames[i]]->joinedRelation);
        if(tbl.find(gn)==tbl.end()) {
            tbl[gn] = relationMap[string(relationNames[i])]->joinedRelation.size() - 1;
        }
        else
            tbl[gn]--;
        i++;
    }

    unordered_map<string,int>::iterator ti=tbl.begin();;
    while( ti!=tbl.end())
    {
        if(ti->second!=0)
        {
            res=false;
            break;
        }
        ti++;
    }
    return res;
}

bool Statistics::helpAttributes(char *v,char *relationNames[], int numberOfJoinAttributes,unordered_map<string,long> &uniqueList)
{
    for(int i=0; i<numberOfJoinAttributes; i++){
        unordered_map<string,RelationInfo*>::iterator itr;
        itr = relationMap.find(relationNames[i]);
        if(relationMap.end() == itr){
            return false;
        }else {
            string relation = string(v);
            if(itr->second->attributeMap.end() != itr->second->attributeMap.find(relation))
            {
                uniqueList[relation]=itr->second->attributeMap.find(relation)->second;
                return true;
            }
        }
    }
    return false;
}


double Statistics::helpTuplesEstimate(struct OrList *orList, unordered_map<string,long> &uniqueList)
{
    unordered_map<string,double> selecMap;
    while(!(orList==NULL && true))
    {
        struct ComparisonOp *comp=orList->left;
        string key = string(comp->left->value);
        if(selecMap.end()==selecMap.find(key)) selecMap[key]=0.0;
        if(!(comp->code != 1 && comp->code != 2)){
            selecMap[key] = selecMap[key]+1.0/3;
        }
        else
        {
            string leftKeyVal = string(comp->left->value);
            long max_val = uniqueList[leftKeyVal];
            if(4==comp->right->code){
                string rightKeyVal = string(comp->right->value);
                if(uniqueList[rightKeyVal] > max_val)
                    max_val = uniqueList[rightKeyVal];
            }
            selecMap[key] = 1.0/max_val + selecMap[key] + 0;
        }
        orList=orList->rightOr;
    }

    double selectivity=1.0;
    unordered_map<string,double>::iterator itr;
    itr = selecMap.begin();
    while(itr!=selecMap.end()) {
        selectivity *= (1.0 - itr->second);
        itr++;
    }
    selectivity = (1.0-selectivity);
    return selectivity;
}




