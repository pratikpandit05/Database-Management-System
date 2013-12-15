#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include<stdlib.h>
#include <string.h>

using namespace std;





DBFile::DBFile () {
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	ofstream metadataFile;
	char metafile_name[100]; // construct path of the metadata file
	sprintf (metafile_name, "%s.md", f_path);
	metadataFile.open (metafile_name);


	if (f_type == heap){
		metadataFile <<"heap";
		myInternalVar=new HeapDBFile;

	}
	else if(f_type == sorted) {
		metadataFile <<"sorted"<<endl;
		metadataFile<<((SortInfo *)startup) ->runLength<<endl;
		((SortInfo*)startup)->myOrder->WriteToMetaFile(metadataFile);
		myInternalVar=new SortedDBFile((SortInfo*)startup);

	}
	else if(f_type == tree){
		metadataFile << "tree"<<endl;
	}
	else{
		cout<<"Unsupported File Type.Exiting..";
		exit(-1);
	}

	myInternalVar->Create(f_path);
	metadataFile.close();
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	myInternalVar->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {
	ifstream metadataFile;
		string f_type;
		char metafile_name[100]; // construct path of the metadata file
		sprintf (metafile_name, "%s.md", f_path);
		metadataFile.open (metafile_name);

		if(!metadataFile.is_open()){
					cerr<<"Meta Data File does not exist.Exiting . . ."<<endl;
					exit(-1);
		}
		getline(metadataFile,f_type);

		if (f_type.compare("heap")==0){
			myInternalVar=new HeapDBFile;
		}


		else if(f_type.compare("sorted")==0) {

			string runLength, whichAtt,numAtt;
			SortInfo *sortInfo=new SortInfo;

			getline(metadataFile,runLength);
			getline(metadataFile,numAtt);


			int runlen=atoi(runLength.c_str());
			int numAtts=atoi(numAtt.c_str());
			if(runlen==0 || numAtts ==0){
				cout<<"Bad!  Corrupt Metadata File.Exiting..."<<endl;
				exit(-1);
			}

			sortInfo->runLength=runlen;
			int whichAtts[numAtts];
			Type whichTypes[numAtts];

			for(int i=0;i<numAtts;i++){

						getline(metadataFile,whichAtt);
						char* att=strtok((char *)whichAtt.c_str(),",");
						char* type=strtok(NULL,",");

						whichAtts[i]= atoi(att);

						if(strcmp(type,"Int")==0)
								whichTypes[i]=Int;
						else if(strcmp(type,"Double")==0)
											whichTypes[i]=Double;
						else if(strcmp(type,"String")==0)
											whichTypes[i]=String;
						else{
								cout<<"Bad!  Corrupt Metadata File.Exiting..."<<endl;
								exit(-1);
						}

			}

			sortInfo->myOrder = new OrderMaker(numAtts, whichAtts, whichTypes);
			//cerr<<"Metadata Info\n";
			//sortInfo->myOrder->Print();
			myInternalVar=new SortedDBFile(sortInfo);
		}

		else if((f_type.compare("tree")==0)){
			cerr << "tree";
		}
		else{
			cout<<"File Type not supported.Exiting..";
			exit(-1);
		}
		myInternalVar->Open(f_path);
}

void DBFile::MoveFirst () {
	myInternalVar->MoveFirst();
}

int DBFile::Close () {

	myInternalVar->Close();
	//delete myInternalVar;
}

void DBFile::Add (Record &rec) {
	myInternalVar->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
	myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	myInternalVar->GetNext(fetchme,cnf,literal);
}
