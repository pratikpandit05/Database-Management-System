#include "Schema.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

extern "C" char* catalog_path;

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


Schema :: Schema(){

	numAtts=-1;
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

Schema::Schema(Schema &left, Schema &right){

       myAtts = new Attribute[left.numAtts+right.numAtts];
       for(int i=0;i<left.numAtts;i++){

                       myAtts[i].name=strdup(left.myAtts[i].name);
                       //strcpy(myAtts[i].name,atts);

                       myAtts[i].myType=left.myAtts[i].myType;

                   //    cerr<<left.myAtts[i].name<<" :"<<left.myAtts[i].myType<<endl;

               }

       //cerr<<"RightTree"<<endl;
       for(int i=0;i<right.numAtts;i++){

                               myAtts[i+left.numAtts].name=strdup(right.myAtts[i].name);
                               myAtts[i+left.numAtts].myType=right.myAtts[i].myType;

          //                                            cerr<<right.myAtts[i].name<<" :"<<right.myAtts[i].myType<<endl;

                       }
       numAtts=left.numAtts+right.numAtts;

}

Schema :: Schema(Schema &oldSchema ,char *alias ){


       myAtts = new Attribute[oldSchema.numAtts];
       for(int i=0;i<oldSchema.numAtts;i++){

               char atts[100];
               strcpy(atts,alias);
               strcat(atts,".");
               strcat(atts,oldSchema.myAtts[i].name);

               myAtts[i].name=new char[100];
               strcpy(myAtts[i].name,atts);

               myAtts[i].myType=oldSchema.myAtts[i].myType;

       }
       numAtts=oldSchema.numAtts;

}

Schema :: Schema(Schema &oldSchema){


       myAtts = new Attribute[oldSchema.numAtts];
       for(int i=0;i<oldSchema.numAtts;i++){

               myAtts[i].name=strdup(oldSchema.myAtts[i].name);
               myAtts[i].myType=oldSchema.myAtts[i].myType;

       }
       numAtts=oldSchema.numAtts;

}









void Schema :: PrintSchema(){

       for(int i=0;i<numAtts;i++){
               cerr<<myAtts[i].name<<" : ";
               if(myAtts[i].myType==0){
                       cerr<<"Int"<<endl;
               }
               else if(myAtts[i].myType==1){
                       cerr<<"Double"<<endl;
               }

               else if(myAtts[i].myType==2){
                       cerr<<"String"<<endl;
               }

       }
}
/*
void Schema :: WriteSchema(char* f_path){
			ofstream schemaFile;
			char schema_name[100]; // construct path of the metadata file
			sprintf (schema_name, "%s.sch", f_path);
			schemaFile.open (schema_name);
			schemaFile<<numAtts<<endl;
          for(int i=0;i<numAtts;i++){
        	  schemaFile<<myAtts[i].name<<endl;
        	  schemaFile<<myAtts[i].myType<<endl;
              }
}
*/

void Schema :: WriteSchema(char* f_path){

			ofstream schemaFile;
			//char schema_name[100]; // construct path of the metadata file
			//sprintf (schema_name, "%s.sch", f_path);
			schemaFile.open (catalog_path,ios::app);
			schemaFile<<"BEGIN"<<endl;
			schemaFile<<f_path<<endl;
			schemaFile<<f_path<<".tbl"<<endl;
			//schemaFile<<numAtts<<endl;
			for(int i=0;i<numAtts;i++){
				schemaFile<<myAtts[i].name<<" ";
				 if(myAtts[i].myType==0){
				                       schemaFile<<"Int"<<endl;
				               }
				               else if(myAtts[i].myType==1){
				                       schemaFile<<"Double"<<endl;
				               }

				               else if(myAtts[i].myType==2){
				                       schemaFile<<"String"<<endl;
				               }
            }
			schemaFile<<"END\n"<<endl;
}

Schema :: Schema (AttList *att, int attCnt){

       myAtts = new Attribute[attCnt+1];
       for(int i=0;i<attCnt+1;i++){

               myAtts[i].name=strdup(att->name);
               if(att->type==0){
                       myAtts[i].myType=Int;
               }
               else if(att->type==1){
                       myAtts[i].myType=Double;
               }
               else{
                       myAtts[i].myType=String;
               }
               att=att->next;
                       }
                       numAtts=attCnt+1;
}

	Schema :: Schema(char* f_path){
		ifstream schemaFile;
		string numAtt;
		char schema_name[100]; // construct path of the metadata file
		sprintf (schema_name, "%s.sch", f_path);
		schemaFile.open (schema_name);


		if(!schemaFile.is_open()){
						cerr<<"Schema File does not exist.Exiting . . ."<<endl;
						exit(-1);
			}
		getline(schemaFile,numAtt);
		numAtts=atoi(numAtt.c_str());
		  myAtts = new Attribute[numAtts];
           string name,type;
          for(int i=0;i<numAtts;i++){
        	  	  	  getline(schemaFile,name);
        	  	  	  myAtts[i].name=strdup((char*)name.c_str());
        	  	    getline(schemaFile,type);
                      myAtts[i].myType=(Type)atoi(type.c_str());

              }
}


Schema :: ~Schema () {
	//for(int i=0;i<numAtts;i++)
		//delete myAtts[i].name;
	//delete fileName;
	delete [] myAtts;
	myAtts = 0;
}

