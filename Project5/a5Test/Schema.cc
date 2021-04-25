#include "Schema.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

int Schema :: Find (char *attName) {

	for (int i = 0; i < numAtts; i++) {
		if (!strcmp (attName, myAtts[i].name)) {
			return i;
		}
	}

	// if we made it here, the attribute was not found
	return -1;
}

Type Schema :: FindType (char *attName) {

	for (int i = 0; i < numAtts; i++) {
		if (!strcmp (attName, myAtts[i].name)) {
			return myAtts[i].myType;
		}
	}

	// if we made it here, the attribute was not found
	return Int;
}

int Schema :: GetNumAtts () {
	return numAtts;
}

Attribute *Schema :: GetAtts () {
	return myAtts;
}


Schema :: Schema (char *fpath, int num_atts, Attribute *atts) {
	fileName = strdup (fpath);
	numAtts = num_atts;
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++ ) {
		if (atts[i].myType == Int) {
			myAtts[i].myType = Int;
		}
		else if (atts[i].myType == Double) {
			myAtts[i].myType = Double;
		}
		else if (atts[i].myType == String) {
			myAtts[i].myType = String;
		} 
		else {
			cout << "Bad attribute type for " << atts[i].myType << "\n";
			delete [] myAtts;
			exit (1);
		}
		myAtts[i].name = strdup (atts[i].name);
	}
}

Schema :: Schema (char *fName, char *relName) {

	FILE *foo = fopen (fName, "r");
	
	// this is enough space to hold any tokens
	char space[200];

	fscanf (foo, "%s", space);
	int totscans = 1;

	// see if the file starts with the correct keyword
	if (strcmp (space, "BEGIN")) {
		cout << "Unfortunately, this does not seem to be a schema file.\n";
		exit (1);
	}	
		
	while (1) {

		// check to see if this is the one we want
		fscanf (foo, "%s", space);
		totscans++;
		if (strcmp (space, relName)) {

			// it is not, so suck up everything to past the BEGIN
			while (1) {

				// suck up another token
				if (fscanf (foo, "%s", space) == EOF) {
					cerr << "Could not find the schema for the specified relation.\n";
					exit (1);
				}

				totscans++;
				if (!strcmp (space, "BEGIN")) {
					break;
				}
			}

		// otherwise, got the correct file!!
		} else {
			break;
		}
	}

	// suck in the file name
	fscanf (foo, "%s", space);
	totscans++;
	fileName = strdup (space);

	// count the number of attributes specified
	numAtts = 0;
	while (1) {
		fscanf (foo, "%s", space);
		if (!strcmp (space, "END")) {
			break;		
		} else {
			fscanf (foo, "%s", space);
			numAtts++;
		}
	}

	// now actually load up the schema
	fclose (foo);
	foo = fopen (fName, "r");

	// go past any un-needed info
	for (int i = 0; i < totscans; i++) {
		fscanf (foo, "%s", space);
	}

	// and load up the schema
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++ ) {

		// read in the attribute name
		fscanf (foo, "%s", space);	
		myAtts[i].name = strdup (space);

		// read in the attribute type
		fscanf (foo, "%s", space);
		if (!strcmp (space, "Int")) {
			myAtts[i].myType = Int;
		} else if (!strcmp (space, "Double")) {
			myAtts[i].myType = Double;
		} else if (!strcmp (space, "String")) {
			myAtts[i].myType = String;
		} else {
			cout << "Bad attribute type for " << myAtts[i].name << "\n";
			exit (1);
		}
	}

	fclose (foo);
}

void Schema::transToAlias(string aliasName) {

	for(int i = 0; i < numAtts; i++) {
		string oldString(myAtts[i].name);
		free(myAtts[i].name);
		string updatedString(aliasName + "." + oldString);
		myAtts[i].name = strdup(updatedString.c_str());
	}

}

void Schema::Print() {

	for(int i = 0; i < numAtts; i++) {
		cout << "Att " << myAtts[i].name << ": ";
		if(myAtts[i].myType == 0)
			cout<<"INT"<<endl;
		else if(myAtts[i].myType == 1)
			cout<<"DOUBLE"<<endl;
		else
			cout<<"STRING"<<endl;
		
	}
	
}

Schema::Schema(Schema *left, Schema *right) {

	int leftAttributes = left->numAtts;
	int rightAttributes = right->numAtts;

	numAtts = leftAttributes + rightAttributes;

	myAtts = new Attribute[numAtts];

	for(int i = 0; i < leftAttributes; i++) {
		myAtts[i] = left->myAtts[i];
	}
	for(int j = 0; j < rightAttributes; j++) {
		myAtts[leftAttributes+j] = right->myAtts[j];  
	}
	
}

Schema::Schema(Schema *sch, vector<int> attributesToKeep) {
	numAtts = attributesToKeep.size();
	myAtts = new Attribute[numAtts];

	for(int i = 0; i < numAtts; i++) {
		myAtts[i] = sch->myAtts[attributesToKeep[i]];
	}

}

Schema :: ~Schema () {
	delete [] myAtts;
	myAtts = 0;
}

