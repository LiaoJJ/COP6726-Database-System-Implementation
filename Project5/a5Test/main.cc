#include <iostream>
#include "ParseTree.h"
#include "NodeForQuery.h"
#include "Statistics.h"
#include <string>
#include <fstream>
#include <sstream>

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct NameList *groupingAtts;		// group by attributes in the predicate
extern struct AndList *boolean;				// the Predicate info from Where clause in SQL query
extern struct TableList *tables; 			// table information form the input SQL query
extern struct FuncOperator *finalFunction;	// information of the aggregate operations in the SQL query
extern struct NameList *attsToSelect;		// attributes in the select clause 
extern int distinctAtts;					// Distinct present in the non aggregate SQL query
extern int distinctFunc;					// Distinct present in the aggregate SQL query

struct AndList* booleanCopy;

extern int queryType;
extern char *outputMode;
extern char *tableName;
extern char *fileToInsert;
extern struct AttrList *attsToCreate;

void pushToVector(vector<AndList>* v) {
	AndList newAnd = *boolean;
	newAnd.rightAnd = 0;
	v->push_back(newAnd);
}

string transOperAndToString(Operand* oprnd) {
	string res;
	string s (oprnd->value);
    stringstream stream;
	for (int i = 0; i < s.size(); i++) {
		if (s[i] == '_') {
			return res;
		}
		else if (s[i] == '.') {
			res = stream.str();
			return res;
		}
		else {
			stream << s[i];
		}
	}
}

void getFinalPlan(vector<AndList>* forJoin, Statistics* statistics) {
	if (forJoin->size() <= 1)
		return;
	vector<AndList> finalPlan;
	while (forJoin->size() > 1) {
		char* firstRelations[] = {&transOperAndToString((*forJoin)[0].left->left->left)[0], &transOperAndToString((*forJoin)[0].left->left->right)[0]};
		double smallest = statistics->Estimate(&(*forJoin)[0], firstRelations, 2);
		int smallestIndex = 0;
		for (int i = 0; i < forJoin->size(); i++) {
			char *relations[] = {&transOperAndToString((*forJoin)[i].left->left->left)[0], &transOperAndToString((*forJoin)[i].left->left->right)[0]};
			double curEstimate = statistics->Estimate(&(*forJoin)[i], relations, 2);
			if (smallest > curEstimate) {
				smallest = curEstimate;
				smallestIndex = i;
			}
		}
		finalPlan.push_back((*forJoin)[smallestIndex]);
		forJoin->erase(forJoin->begin() + smallestIndex);
	}
	finalPlan.push_back((*forJoin)[0]);
	*forJoin = finalPlan;
}

NodeForQuery* retrieveAncient(NodeForQuery* curNode) {
	if (curNode->parentNode != NULL)
		return retrieveAncient(curNode->parentNode);
	else
		return curNode;
}

void PrintParseTree (struct AndList *andPointer) {

    cout << "(";

    while (andPointer) {

        struct OrList *orPointer = andPointer->left;

        while (orPointer) {

            struct ComparisonOp *comPointer = orPointer->left;

            if (comPointer!=NULL) {

                struct Operand *pOperand = comPointer->left;

                if(pOperand!=NULL) {

                    cout<<pOperand->value<<"";

                }

                switch(comPointer->code) {

                    case LESS_THAN:
                        cout<<" < "; break;
                    case GREATER_THAN:
                        cout<<" > "; break;
                    case EQUALS:
                        cout<<" = "; break;
                    default:
                        cout << " unknown code " << comPointer->code;

                }

                pOperand = comPointer->right;

                if(pOperand!=NULL) {

                    cout<<pOperand->value<<"";
                }

            }

            if(orPointer->rightOr) {

                cout<<" OR ";

            }

            orPointer = orPointer->rightOr;

        }

        if(andPointer->rightAnd) {

            cout<<") AND (";
        }

        andPointer = andPointer->rightAnd;

    }

    cout << ")" << endl;

}


int main () {
    outputMode = "STDOUT";
    while(1) {
        Statistics statistics;
        statistics.Read("OriginTables.txt");
        int pipeID = 1;
        cout<<endl;
        cout << "Enter your SQL: ";
        cout<<endl;
        vector <AndList> forJoin, forSelect;
        yyparse();
        cin.clear();
        clearerr(stdin);
        cout << "\nYou Sql has been parsed" << endl;

        if(queryType==2){
            // Create
            char filePath[100];
            char tpchPath[100];

            sprintf (filePath, "bin/%s.bin", tableName);
            sprintf (tpchPath, "%s.tbl", tableName);

            ofstream ofs("catalog", ifstream :: app);

            ofs<< endl;
            ofs << "BEGIN" << endl;
            ofs << tableName << endl;
            ofs << tpchPath <<endl;

            while (attsToCreate) {
                ofs << attsToCreate->name << " ";
                switch (attsToCreate->type) {
                    case 0:
                        ofs << "Int" << endl;
                        break;
                    case 1:
                        ofs << "Double" << endl;
                        break;
                    case 2:
                        ofs << "String" << endl;
                        break;
                    default:
                        cout<< "error" << endl;
                        break;
                }
                attsToCreate = attsToCreate->next;
            }
            ofs << "END" << endl;
            ofs.close();
            DBFile file;
            file.Create (filePath, heap, NULL);
            file.Close();
        }
        else if(queryType==3){
            // DDROP
            char filePath[100];
            char metaPath[100];

            sprintf (filePath, "bin/%s.bin", tableName);
            sprintf (metaPath, "%s.meta", filePath);

            remove (filePath);
            remove (metaPath);

            ostringstream text;
            ifstream in_file("catalog");

            text << in_file.rdbuf();
            string str = text.str();
            string str_search = string(tableName);
            string str_replace = "\n";
            size_t pos1 = str.find(string(tableName));
            size_t pos2 = str.find("END", pos1);
            str.replace(pos1-8, pos2+4, str_replace);
            in_file.close();

            ofstream out_file("catalog");
            out_file << str;
            out_file.close();
        }
        else if(queryType==4){
            // Insert
            char filePath[100];
            char tpchPath[100];

            sprintf (filePath, "bin/%s.bin", tableName);
            sprintf (tpchPath, "tpch/%s.tbl", tableName);
            DBFile file;
            Schema schema("catalog", tableName);
            file.Open(filePath);
            file.Load(schema, tpchPath);
            file.Close();
        }
        else if(queryType==5){
            // SET
        }
        else if(queryType==1) {
            // iterating over the predicate information
            OrList *cur;
            while (boolean != NULL) {
                cur = boolean->left;
                //Separate predicate that used in join operation
                bool selfAndChildValid = (cur != NULL && cur->left->code == EQUALS);
                bool grandChildValid = (cur->left->left->code == NAME && cur->left->right->code == NAME);
                if (selfAndChildValid && grandChildValid) {
                    pushToVector(&forJoin);
                }
                    //Separate predicate that used in Select
                else {
                    pushToVector(&forSelect);
                }
                boolean = boolean->rightAnd;
            }

            unordered_map < string, NodeForQuery * > nodeMap;
            NodeForQuery *curNode = NULL;
            NodeForQuery *orginNode = NULL;

            //Create SelectFile nodes
            TableList *iterTables = tables;
            while (iterTables != NULL) {
                Schema *schemaForTable = new Schema("catalog", iterTables->tableName);
                nodeMap[iterTables->tableName] = new SelectFileNode(pipeID++, schemaForTable);
                if (iterTables->aliasAs != NULL) {
                    Schema *schemaForalias = new Schema("catalog", iterTables->tableName);
                    statistics.CopyRel(iterTables->tableName, iterTables->aliasAs);
                    schemaForalias->transToAlias(string(iterTables->aliasAs));
                    nodeMap[iterTables->aliasAs] = new SelectFileNode(pipeID++, schemaForalias);
//            char *cstr = ;
                    char filepath[100];
                    sprintf(filepath, "bin/%s.bin", iterTables->tableName);
//            cout<<filepath<<endl;
                    ((SelectFileNode *) nodeMap[iterTables->aliasAs])->file.Open(filepath);
                }
                iterTables = iterTables->next;
            }


            //Give CNF to SelectFile nodes for filter
            for (int i = 0; i < forSelect.size(); i++) {
                string relationName =
                        forSelect[i].left->left->left->code == NAME ? transOperAndToString(
                                forSelect[i].left->left->left)
                                                                    : transOperAndToString(
                                forSelect[i].left->left->right);
                SelectFileNode *tempNode = (SelectFileNode *) nodeMap[relationName];
                tempNode->andList = &(forSelect[i]);
                tempNode->needToPrintCNF = true;
                orginNode = tempNode;
            }



            //Compute Optimal soludtion for query
            getFinalPlan(&forJoin, &statistics);
            cout << "Optimal Plan has been built" << endl;
            //Create Join nodes
            for (int i = 0; i < forJoin.size(); i++) {
                string relOne = transOperAndToString(forJoin[i].left->left->left), relTwo = transOperAndToString(
                        forJoin[i].left->left->right);
                string relationName = relOne;
                NodeForQuery *leftChild = nodeMap[relOne];
                NodeForQuery *rightChild = nodeMap[relTwo];
                leftChild = retrieveAncient(leftChild);
                rightChild = retrieveAncient(rightChild);

                curNode = new JoinNode(pipeID++, new Schema(leftChild->outputSchema, rightChild->outputSchema),
                                       &forJoin[i]);
                curNode->makeChild(leftChild, true);
                curNode->makeChild(rightChild, false);
                orginNode = curNode;

            }

            //Create DuplicateRemovalNodes on aggregate attribute
            if (finalFunction != NULL && distinctFunc == 1) {
                curNode = new DuplicateRemovalNode(pipeID++, orginNode->outputSchema);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }

            //Create SumNode
            if (finalFunction != NULL && groupingAtts == NULL) {
                curNode = new SumNode(pipeID++, NULL, finalFunction);
                ((SumNode *) curNode)->function.GrowFromParseTree(finalFunction, *orginNode->outputSchema);
                Attribute *DA;
                if (((SumNode *) curNode)->function.returnInt()) {
                    DA = new Attribute{"SUM", Int};
                } else {
                    DA = new Attribute{"SUM", Double};
                }
                ((SumNode *) curNode)->outputSchema = new Schema("out_sch", 1, DA);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }
            //Create GroupByNode
            if (finalFunction != NULL && groupingAtts != NULL) {
                curNode = new GroupByNode(pipeID++, orginNode->outputSchema, groupingAtts, finalFunction);
                ((GroupByNode *) curNode)->function = new Function(((GroupByNode *) curNode)->generateFunction());
                ((GroupByNode *) curNode)->orderMaker = new OrderMaker(((GroupByNode *) curNode)->generateOrderMaker());
                Attribute *DA;
                if (((GroupByNode *) curNode)->function->returnInt()) {
                    DA = new Attribute{"SUM", Int};
                } else {
                    DA = new Attribute{"SUM", Double};
                }
                ((GroupByNode *) curNode)->outputSchema = new Schema("out_sch", 1, DA);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }
            //Create DuplicateRemovalNodes on non-aggregate attribute
            if (distinctAtts == 1) {
                curNode = new DuplicateRemovalNode(pipeID++, orginNode->outputSchema);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }

            //Create Project nodes
            if (attsToSelect != NULL) {
                vector<int> attributesKeep;
                NameList *iterAtt = attsToSelect;
                string attribute;
                while (iterAtt != 0) {
                    attributesKeep.push_back(orginNode->outputSchema->Find(const_cast<char *>(iterAtt->name)));
                    iterAtt = iterAtt->next;
                }
                Schema *newSchema = new Schema(orginNode->outputSchema, attributesKeep);
                curNode = new ProjectNode(pipeID++, newSchema, attributesKeep);
                curNode->makeChild(orginNode, true);
                orginNode = curNode;
            }
            cout << endl;
            cout << "Current OUTPUT MODE: " << outputMode << endl;
            cout<<"--------------"<< endl;

            if(strcmp(outputMode, "NONE")==0){
                curNode->printInOrder();
            }
            else{
                if(strcmp(outputMode, "STDOUT")==0){
                    curNode->Apply();
                    Pipe *p = curNode->pipe;
                    Record rec1;
                    int i = 0;
                    while (p->Remove(&rec1)) {
                        i++;
                        cout << "-------------------" << endl;
                        rec1.Print(curNode->outputSchema);
                    }
                    cout << "Total Count: " << i << endl;
                }
                else{
                    curNode = new WriteOutNode(pipeID++, orginNode->outputSchema);
                    ((WriteOutNode*)curNode)->filePath = outputMode;
                    curNode->makeChild(orginNode, true);
                    orginNode = curNode;
                    curNode->Apply();
                }
            }



            for (auto it = nodeMap.begin(); it != nodeMap.end(); it++) {
                delete it->second;
            }
//        TableList *iterTables1 = tables;
//        while (iterTables1 != NULL) {
//            char filepath[100];
//            sprintf(filepath, "bin/%s.bin", iterTables1->tableName);
//            if (iterTables1->aliasAs != NULL) {
//                ((SelectFileNode *) nodeMap[iterTables1->aliasAs])->file.Close();
//            }
//            iterTables1 = iterTables1->next;
//        }

//    DBFile file;
//    CNF cnf;
//    Schema sch ("catalog", "nation");
//    sch.transToAlias("n");
//    Record literal;
//    cnf.GrowFromParseTree(booleanCopy, &sch, literal);
//    cout<< booleanCopy<<endl;
//    file.Create("bin/nation.bin", heap, NULL);
//    file.Load(sch, "tpch/nation.tbl");
//    Record rec;
//    while(file.GetNext(rec, cnf, literal)){
//        rec.Print(&sch);
//    }
//    file.Close();
        }
    }
}


