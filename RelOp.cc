#include "RelOp.h"
#include "BigQ.h"
//select File
typedef struct {
		    DBFile *inFile;
			Pipe *outPipe;
			CNF *selOp;
			Record *literal;
		}sfUtil;
	//sfUtil *sfutil;



void *sfworkerFunction(void* arg){

		sfUtil *util = (sfUtil *) arg;
		Record *buffer = new Record;

			ComparisonEngine ceng;
			DBFile *dbfile = util->inFile;
			dbfile->MoveFirst ();
			int counter=0,inserted=0;

			//util->selOp->Print();

			while (dbfile->GetNext (*buffer,*(util->selOp),*(util->literal))) {
				counter ++;
				if (counter%100000 == 0) {
					 cerr <<"."<< endl;
				}
				util->outPipe->Insert(buffer);
				inserted++;

			}
			util->outPipe->ShutDown();
			delete buffer;
			delete util;
			//cerr << "Select File: Inserted " << inserted << " recs into the pipe\n";
	}
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	    sfUtil *sfutil=new sfUtil{&inFile, &outPipe, &selOp, &literal};
		pthread_create(&thread, NULL, sfworkerFunction, sfutil);
	}


void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}




// Select Pipe

typedef struct {
			    Pipe *inPipe;
				Pipe *outPipe;
				CNF *selOp;
				Record *literal;
	}spUtil;
	//spUtil *sputil;


	void *spworkerFunction(void *arg){


		spUtil *util = (spUtil *) arg;
		int counter=0,inserted=0;
		Record *buffer=new Record;
		ComparisonEngine ceng;
		while(util->inPipe->Remove(buffer)){
			counter ++;
						if (counter%100000 == 0) {
							 cerr <<"."<< endl;
						}
						if (ceng.Compare (buffer, util->literal,util->selOp)){								//if the CNF is matching with the record
									util->outPipe->Insert(buffer);
									inserted++;
						}
		}
		util->outPipe->ShutDown();
		delete buffer;
		delete util;
		//cerr << " Select Pipe: Scanned "<<counter<<" records. Inserted " << inserted << " recs into the pipe\n";
	}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

	spUtil *sputil=new spUtil{&inPipe, &outPipe, &selOp, &literal};
	pthread_create(&thread, NULL, spworkerFunction, sputil);

}

void SelectPipe::WaitUntilDone () {
	 pthread_join (thread, NULL);
}


void SelectPipe::Use_n_Pages (int runlen) {

}


//Project

typedef struct {
			    Pipe *inPipe;
				Pipe *outPipe;
				int *keepMe;
				int numAttsInput;
				int numAttsOutput;
	}pUtil;

void * pworkerFunction (void *arg){



	pUtil *util = (pUtil *) arg;
	int counter=0,inserted=0;
	Record *buffer=new Record;
	//ComparisonEngine ceng;

	while(util->inPipe->Remove(buffer)){
			counter ++;
			if (counter%100000 == 0) {
					 cerr <<"."<< endl;
				}

			buffer->Project(util->keepMe,util->numAttsOutput,util->numAttsInput);
			util->outPipe->Insert(buffer);
			inserted++;
		}

		util->outPipe->ShutDown();
		delete buffer;
		delete util;
		//cerr << "Project:   Scanned "<<counter<<" records.Project: Inserted " << inserted << " recs into the pipe\n";

}
void Project :: Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
	pUtil *putil=new pUtil{&inPipe, &outPipe, keepMe,numAttsInput,numAttsOutput};
	pthread_create(&thread, NULL, pworkerFunction, putil);
}


void Project::WaitUntilDone () {
	 pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int runlen) {

}

typedef struct {
				    Pipe *inPipeL;
					Pipe *inPipeR;
					Pipe *outPipe;
					CNF *selOp;
					Record *literal;
					int runlen;
		}jUtil;
		void *jworkerFunction(void *arg){
			/*
						Attribute IA = {"int", Int};
						Attribute SA = {"string", String};
						Attribute DA = {"double", Double};
						Attribute ps_supplycost = {"ps_supplycost", Double};
						Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA};
						Attribute satt[]={IA,SA,SA,IA,SA,DA,SA};
						Attribute psatt[] = {IA,IA,IA,ps_supplycost,SA};

						Schema join_sch ("join_sch", 12, joinatt);
						Schema ps_sch("ps_sch",5,psatt);
						Schema s_sch("s_sch",7,satt);

			*/


				jUtil *util = (jUtil *) arg;
				Record *buffer = new Record;
			   ComparisonEngine ceng;
			   int buffsz = 100; // pipe cache size
			   Pipe *outPipeL=new Pipe(buffsz);
			   Pipe *outPipeR=new Pipe(buffsz);
			   OrderMaker orderMakerL;
			   OrderMaker orderMakerR;
			   Record * left=new Record;
			   Record * right=new Record;

           	 vector<Record*> leftblock;
        	 vector<Record*> rightblock;

       	     Record *result=new Record;
       	     int le=0,ri=0,bo=0;
             int numOfAttsL;
       	     int numOfAttsR;

             if(util->selOp->GetSortOrders(orderMakerL,orderMakerR)){

            	//orderMakerL.Print();
             //cerr<<util->runlen;
             BigQ bqL (*(util->inPipeL), *outPipeL, orderMakerL, util->runlen);


             BigQ bqR (*(util->inPipeR), *outPipeR, orderMakerR, util->runlen);


             //ComparisonEngine ceng;
             outPipeL->Remove(left);

             outPipeR->Remove(right);

             bool leftEnd=false;
             bool rightEnd=false;
             numOfAttsL=((int *) left->bits)[1] / sizeof(int) -1;
             numOfAttsR=((int *) right->bits)[1] / sizeof(int) -1;
             //cerr<<"numOfAttsL "<<numOfAttsL<<"numOfAttsR "<<numOfAttsR<<endl;
             int attsToKeep[numOfAttsL+numOfAttsR];

             for(int i=0;i<numOfAttsL;i++){
                                         attsToKeep[i]=i;
                            }
            for(int i=0;i<numOfAttsR;i++){
                          attsToKeep[numOfAttsL+i]=i;
             }
             while (true){
          	   if(ceng.Compare(left,&orderMakerL,right,&orderMakerR)<0){
          		   le++;
          		   if(!outPipeL->Remove(left))
          			   break;
          		  //cerr<<"removed from L"<<endl;
          	   }

          	   else if(ceng.Compare(left,&orderMakerL,right,&orderMakerR)>0){
          		   ri++;
          		   if(!outPipeR->Remove(right))
          		   	   break;
          		   //cerr<<"removed from R"<<endl;
          	   }

          	   else{

          		   	   	   	  leftEnd=false;
          		   	   	   	  rightEnd=false;


          		   	   	   	   do{
											le++;
											Record *prev=new Record;
											prev->Consume(left);
											leftblock.push_back(prev);
											if(!(outPipeL->Remove (left))){
												leftEnd=true;
												break;
											}
											if(ceng.Compare (prev, left,&orderMakerL) !=0){
												break;
											}
          		   	   	   	  }while(true);

								   do{
									   	   ri++;
											Record *prev=new Record;
											prev->Consume(right);
											rightblock.push_back(prev);
											if(!(outPipeR->Remove (right))){
												rightEnd=true;
												break;
											}
											if(ceng.Compare (prev,right,&orderMakerR) !=0){
												break;
											}
								  }while(true);

						 		for (int i = 0; i < leftblock.size(); i++) {
						 			for (int j = 0; j < rightblock.size(); j++) {
						 				if(ceng.Compare(leftblock.at(i),rightblock.at(j),util->literal,util->selOp)==1){
											result->MergeRecords(leftblock.at(i),rightblock.at(j),numOfAttsL,numOfAttsR,attsToKeep,numOfAttsL+numOfAttsR,numOfAttsL);
											//result->Print(&join_sch);
											bo++;
											util->outPipe->Insert(result);
						 				}
						 			}
						 			delete leftblock.at(i);
						 		}

								for (int j = 0; j < rightblock.size(); j++) {
									delete rightblock.at(j);
								}


						 		leftblock.clear();
						 		rightblock.clear();
						   if(rightEnd || leftEnd)
										   break;
          	   }
             }

             while(outPipeL->Remove(left)){
          	   le++;
             }

             while(outPipeR->Remove(left)){
          	   ri++;
                            }

            // cerr<<"le: "<<le<<" ri: "<<ri<<" bo:"<<bo<<endl;
			}

		else{
			//do block nested join
			//push all records of right pipe into vector
			int max_size = (util->runlen) * PAGE_SIZE;
			static int bnCount;
				std::time_t start = std::clock();

				char tempFile[100];
				sprintf(tempFile,"bnFile%d.tmp",bnCount++);


			DBFile dbfile;
			dbfile.Create(tempFile,heap,NULL);

			util->inPipeR->Remove(right);

			numOfAttsR=((int *) right->bits)[1] / sizeof(int) -1;

			do{
				Record *temp=new Record;
				temp->Consume(right);
				dbfile.Add(*temp);
				//rightblock.push_back(temp);
			}while(util->inPipeR->Remove(right));

			dbfile.Close();
			dbfile.Open(tempFile);

			util->inPipeL->Remove(left);

			numOfAttsL=((int *) left->bits)[1] / sizeof(int) -1;

		    int attsToKeep[numOfAttsL+numOfAttsR];

		   for(int i=0;i<numOfAttsL;i++){
									   attsToKeep[i]=i;
						  }
		  for(int i=0;i<numOfAttsR;i++){
						attsToKeep[numOfAttsL+i]=i;
		   }
		 int inserted=0;
		 int count=1;
		int curSizeInBytesLeft = sizeof(int);
		int recSize = 0;
		//left relation
		leftblock.clear();
		int counterLeft=0;
		do{
						counterLeft++;
						recSize= (left)->GetSize();
						Record *newrecleft=new Record;
						newrecleft->Consume(left);

						if (curSizeInBytesLeft + recSize <=PAGE_SIZE) {
								leftblock.push_back(newrecleft);
								curSizeInBytesLeft += recSize;
						}
						else{

																										int counterRight=0;
																										int curSizeInBytes = sizeof(int);
																										recSize = 0;
																										dbfile.MoveFirst();
																										rightblock.clear();
																										while(dbfile.GetNext(*right)){
																											counterRight++;

																													recSize = (right)->GetSize(); //((int *) b)[0];
																													Record *newrec = new Record;
																													newrec->Consume(right);

																													//If total size has not exceeded max size of run,
																													//Increase current size of current run and
																													//push record into Priority Queue
																													if (curSizeInBytes + recSize <= max_size) {

																														rightblock.push_back(newrec);

																														curSizeInBytes += recSize;
																													}
																													else
																																			{


																																			for(int i=0;i<leftblock.size();i++){

																																				for (int j = 0; j < rightblock.size(); j++) {


																																					if(ceng.Compare(leftblock.at(i),rightblock.at(j),util->literal,util->selOp)==1){
																																					result->MergeRecords(leftblock.at(i),rightblock.at(j),numOfAttsL,numOfAttsR,attsToKeep,numOfAttsL+numOfAttsR,numOfAttsL);

																																					util->outPipe->Insert(result);

																																					inserted++;
																																					}

																																				}
																																			}
																																			for(int k=0;k<rightblock.size();k++){
																																				delete rightblock.at(k);
																																			}
																																			rightblock.clear();
																																			rightblock.push_back(newrec);
																																			curSizeInBytes = sizeof(int);
																																			recSize = 0;


																														}

																										}
																										if(rightblock.size()>0){

																												for(int i=0;i<leftblock.size();i++){
																														for (int j = 0; j < rightblock.size(); j++) {

																															if(ceng.Compare(leftblock.at(i),rightblock.at(j),util->literal,util->selOp)==1){
																																	result->MergeRecords(leftblock.at(i),rightblock.at(j),numOfAttsL,numOfAttsR,attsToKeep,numOfAttsL+numOfAttsR,numOfAttsL);
																																	util->outPipe->Insert(result);
																																	inserted++;
																															}
																														}
																													}

																												for(int k=0;k<rightblock.size();k++){
																													delete rightblock.at(k);
																												}

																												rightblock.clear();

																										}

										for(int i=0;i<leftblock.size();i++){
											delete leftblock.at(i);
										}
										leftblock.clear();
										leftblock.push_back(newrecleft);
										curSizeInBytesLeft = sizeof(int);

						}

					}while(util->inPipeL->Remove(left));

			if(leftblock.size()>0){

									int curSizeInBytes = sizeof(int);
									recSize = 0;
									dbfile.MoveFirst();
									while(dbfile.GetNext(*right)){
												recSize = (right)->GetSize(); //((int *) b)[0];
												Record *newrec = new Record;
												newrec->Consume(right);

												//If total size has not exceeded max size of run,
												//Increase current size of current run and
												//push record into Priority Queue
												if (curSizeInBytes + recSize <= max_size) {
													rightblock.push_back(newrec);

													curSizeInBytes += recSize;
												}
												else
													{
																		for(int i=0;i<leftblock.size();i++){
																			for (int j = 0; j < rightblock.size(); j++) {
																				if(ceng.Compare(leftblock.at(i),rightblock.at(j),util->literal,util->selOp)==1){
																						result->MergeRecords(leftblock.at(i),rightblock.at(j),numOfAttsL,numOfAttsR,attsToKeep,numOfAttsL+numOfAttsR,numOfAttsL);
																						util->outPipe->Insert(result);
																						inserted++;
																				}
																			}
																		}
																		for(int k=0;k<rightblock.size();k++){
																			delete rightblock.at(k);
																		}
																		rightblock.clear();
																		rightblock.push_back(newrec);
																		curSizeInBytes = sizeof(int);
																		recSize = 0;

													}

									}
									if(rightblock.size()>0){


									for(int i=0;i<leftblock.size();i++){
											for (int j = 0; j < rightblock.size(); j++) {
												if(ceng.Compare(leftblock.at(i),rightblock.at(j),util->literal,util->selOp)==1){
														result->MergeRecords(leftblock.at(i),rightblock.at(j),numOfAttsL,numOfAttsR,attsToKeep,numOfAttsL+numOfAttsR,numOfAttsL);
														util->outPipe->Insert(result);
														inserted++;
												}
											}
										}
									for(int k=0;k<rightblock.size();k++){
										delete rightblock.at(k);
									}
									rightblock.clear();
							}
									for(int i=0;i<leftblock.size();i++){
												delete leftblock.at(i);
									}
									leftblock.clear();
									//leftblock.push_back();

			}

				dbfile.Close();
				//cerr << "Time elapsed: " << static_cast<double> (clock() - start)/ CLOCKS_PER_SEC << " secs\n";
				//cerr<<"Block Nested Join Completed:  inserted "<<inserted<<"  records"<<endl;
				remove(tempFile);
		}


               util->outPipe->ShutDown();
               delete outPipeL;
               delete outPipeR;
               delete result;
               delete util;
	}

void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){

	jUtil *jutil=new jUtil{&inPipeL, &inPipeR, &outPipe, &selOp,&literal,runlen};
	pthread_create(&thread, NULL, jworkerFunction, jutil);

}
void Join:: WaitUntilDone () {
	 pthread_join (thread, NULL);
}
void Join:: Use_n_Pages (int n){
		runlen=n;

}

	//SUM
typedef struct {
						Pipe *inPipe;
						Pipe *outPipe;
						Function *computeMe;
	}sUtil;




void * sworkerFunction (void *arg){
	Record *sumRecord = new Record;
	Type type;

	sUtil *util = (sUtil *) arg;
	if(util->computeMe->returnsInt)
		type=Int;
	else
		type=Double;

	//Schema out_sch ("out_sch", 1, att1);
	cerr<<"Sum called";

	int counter=0,inserted=0;
	Record *buffer=new Record;
	//ComparisonEngine ceng;
	int intSum=0;
	double doubleSum=0;
	int argIntSum;
	double argDoubleSum;


	while(util->inPipe->Remove(buffer)){
			counter ++;
			if (counter%100000 == 0) {
					 cerr <<"."<< endl;
				}
			util->computeMe->Apply(*buffer,argIntSum,argDoubleSum);


			if(util->computeMe->returnsInt){

				intSum+=argIntSum;
			}

			else{
					doubleSum+=argDoubleSum;

			}

		}
		sumRecord->CreateRecord(type,intSum,doubleSum);
		util->outPipe->Insert(sumRecord);
		util->outPipe->ShutDown();
		delete buffer;
		delete util;
		//cerr << "Sum:   Scanned "<<counter<<" records.\n";
}
void Sum :: Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	sUtil *sutil=new sUtil{&inPipe, &outPipe, &computeMe};
	pthread_create(&thread, NULL, sworkerFunction, sutil);
}


void Sum::WaitUntilDone () {
	 pthread_join (thread, NULL);
}

void Sum::Use_n_Pages (int runlen) {

}

typedef struct {
		Pipe *inPipe;
		Pipe *outPipe;
		Schema *mySchema;
		int runlen;
}drUtil;
void * drworkerFunction (void *arg){
		drUtil *util=(drUtil*) arg;
		OrderMaker *myOrdermaker=new OrderMaker(util->mySchema);
		int buffsz = 100; // pipe cache size
		Pipe *outPipe=new Pipe(buffsz);
		BigQ bqL (*(util->inPipe), *outPipe, *myOrdermaker, util->runlen);

		 Record rec[2];
		 Record *last = NULL, *prev = NULL;
		 ComparisonEngine ceng;
		 int counter=0;
		 int inserted=0;

		 while (outPipe->Remove (&rec[counter%2])) {

				prev = last;
				last = &rec[counter%2];

				if (prev && last && ceng.Compare (prev, last, myOrdermaker) !=0) {
						util->outPipe->Insert(prev);
						inserted++;
					}
				counter++;
				}
		 util->outPipe->Insert(last);
		 inserted++;
		 util->outPipe->ShutDown();
		//cout << "Duplicate Removal: Scanned "<<counter<<" records. " << inserted << " unique recs \n";

}
void DuplicateRemoval:: Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	drUtil *drutil=new drUtil {&inPipe, &outPipe, &mySchema,runlen};
	pthread_create(&thread, NULL, drworkerFunction, drutil);
}


void DuplicateRemoval::WaitUntilDone () {
	 pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n) {
		runlen=n;
}

//Write to file

typedef struct {
				Pipe *inPipe;
				FILE *outFile;
				Schema *mySchema;
}wfUtil;


void *wfworkerFunction(void* arg){
	wfUtil *util = (wfUtil *) arg;
	Record *buffer=new Record;
	int counter=0;

	while(util->inPipe->Remove(buffer)){
		buffer->WriteToFile(util->mySchema,util->outFile);
		counter++;

	}
	fclose(util->outFile);
	//cerr<<"Write to file: Scanned " <<counter<<" records\n";
}


void WriteOut:: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	wfUtil *wfutil=new wfUtil{&inPipe,outFile, &mySchema};
	pthread_create(&thread,NULL, wfworkerFunction, wfutil);
}
void WriteOut:: WaitUntilDone () {
	 pthread_join (thread, NULL);
}
void WriteOut:: Use_n_Pages (int n) { }

//Group By
typedef struct {
				Pipe *inPipe;
				Pipe *outPipe;
				OrderMaker *groupAtts;
				Function *computeMe;
				int runlen;
}gbUtil;
void * gbworkerFunction (void *arg){
		gbUtil *util=(gbUtil*) arg;
		Record *sumRecord = new Record;
		Record *recLeft = new Record;
		Record *recRight = new Record;
		Type type;
		int buffsz = 100; // pipe cache size
		Pipe *outPipe=new Pipe(buffsz);

		BigQ bq (*(util->inPipe), *outPipe, *(util->groupAtts), util->runlen);

		Record *temp=new Record;
/*

		Attribute IA = {"int", Int};
		Attribute SA = {"string", String};
		Attribute DA = {"double", Double};
		Attribute ps_supplycost = {"ps_supplycost", Double};
		Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA};
		Schema join_sch ("join_sch", 12, joinatt);
*/
		Record rec[2] ;
		Record *last = NULL, *prev = NULL;
		ComparisonEngine ceng;
		int counter=0;
		int inserted=0;
		int intSum=0;
		double doubleSum=0;
		int argIntSum;
		double argDoubleSum;
		if(util->computeMe->returnsInt)
			type=Int;
		else
			type=Double;

		int attsToKeep[util->groupAtts->numAtts+1];
		attsToKeep[0]=0;

		 for(int i=1;i<=util->groupAtts->numAtts;i++){
			 	 attsToKeep[i]=util->groupAtts->whichAtts[i-1];
		 }

		 while (outPipe->Remove (&rec[counter%2])) {

				prev = last;
				last = &rec[counter%2];

				if(prev && last){
					if (ceng.Compare (prev, last, util->groupAtts) !=0 ) {
					//insert aggregate tuple in pipe

						util->computeMe->Apply(*prev,argIntSum,argDoubleSum);
						if(util->computeMe->returnsInt)
								intSum+=argIntSum;
						else
								doubleSum+=argDoubleSum;
						recLeft->CreateRecord(type,intSum,doubleSum);


						sumRecord->MergeRecords(recLeft,prev,1 ,((int *) prev->bits)[1] / sizeof(int) -1, attsToKeep, (util->groupAtts->numAtts)+1,1);
						util->outPipe->Insert(sumRecord);
						intSum=0;doubleSum=0;

						inserted++;
							}
					else{
							util->computeMe->Apply(*prev,argIntSum,argDoubleSum);
							if(util->computeMe->returnsInt)
										intSum+=argIntSum;
							else
										doubleSum+=argDoubleSum;

							}

				}
					counter++;
				}

		 	 //Process and insert last record
			util->computeMe->Apply(*last,argIntSum,argDoubleSum);
					if(util->computeMe->returnsInt)
							intSum+=argIntSum;
					else
			doubleSum+=argDoubleSum;
			//last->Project(util->groupAtts->whichAtts,util->groupAtts->numAtts,((int *) prev->bits)[1] / sizeof(int) -1);
			recLeft->CreateRecord(type,intSum,doubleSum);

			sumRecord->MergeRecords(recLeft,last,1 , ((int *) prev->bits)[1] / sizeof(int) -1, attsToKeep, util->groupAtts->numAtts+1,1);
			util->outPipe->Insert(sumRecord);
			inserted++;
			//

		 util->outPipe->ShutDown();
		//cout << "Group By: Scanned "<<counter<<" records. " << inserted << " unique recs \n";

}
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	gbUtil *gbutil=new gbUtil {&inPipe, &outPipe, &groupAtts,&computeMe,runlen};
	pthread_create(&thread, NULL, gbworkerFunction, gbutil);
}


void GroupBy::WaitUntilDone () {
	 pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages (int n) {
		runlen=n;
}




