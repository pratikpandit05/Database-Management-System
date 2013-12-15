#include "Nodes.h"

using namespace std;

extern Pipe *pipes[20];


	void SelectNode::PrintNode(){

			cout<<"*****************************************"<<endl;

			cout<<opName<<endl;
			cout<<"OutPut pipe ID :"<<outPipe_Id<<endl;
			cout<<"Output Schema :"<<endl;
			schema->PrintSchema();
			cout<<endl;

			if(cnf!=NULL){
					cout<<opName<<"  CNF"<<endl;
					cnf->Print();
					cout<<endl;
			}
	}

	 void SelectNode:: Execute(){
		 char rpath[100];
		sprintf (rpath, "%s.bin", relName);

		SelectFile *sf= new SelectFile;
		dbf=new DBFile;
		dbf->Open(rpath);
		sf->Use_n_Pages(RUNLEN);
		outPipe=pipes[outPipe_Id];

		sf->Run(*dbf,*outPipe,*cnf,*literal);
		//clear_pipe(false);
		//sf.WaitUntilDone();
		relop=sf;
	 }
	 int SelectNode::clear_pipe (bool print) {
			 	 	Record rec;
			 	 	int cnt = 0;
			 	 	while (outPipe->Remove (&rec)) {
			 	 		if (print) {
			 	 			rec.Print (schema);
			 	 		}
			 	 		cnt++;
			 	 		if(cnt%1000==0)
			 	 			cerr<<".";
			 	 	}
			 	 	return cnt;
			 	 }



	void JoinNode ::  PrintNode(){

   		cout<<"*****************************************"<<endl;

   		cout<<opName<<endl;

   			cout<<"Input Pipe:"<<leftPipe_Id<<endl;
   			cout<<"Input Pipe:"<<rightPipe_Id<<endl;
   		cout<<"OutPut pipe ID :"<<outPipe_Id<<endl;
   		cout<<"Output Schema :"<<endl;
   		schema->PrintSchema();
   		cout<<endl;

		if(cnf!=NULL){
			cout<<opName<<"  CNF"<<endl;
			cnf->Print();
			cout<<endl;
		}
	}

	void JoinNode::Execute(){
		Join *J=new Join;
		J->Use_n_Pages(RUNLEN);
		J->Run(*pipes[leftPipe_Id],*pipes[rightPipe_Id],*pipes[outPipe_Id],*cnf,*literal);
		relop=J;
	}

	void SumNode::PrintNode(){

   		cout<<"*****************************************"<<endl;

   		cout<<opName<<endl;

		cout<<"Input Pipe:"<<leftPipe_Id<<endl;
   		cout<<"OutPut pipe ID :"<<outPipe_Id<<endl;
   		cout<<"Output Schema :"<<endl;
   		schema->PrintSchema();
   		cout<<endl;

		cout<<opName<<"  Function"<<endl;
		function->Print();
		cout<<endl;

	}

	void SumNode:: Execute(){
			Sum *T = new Sum;
			T->Use_n_Pages(RUNLEN);
			T->Run(*pipes[leftPipe_Id],*pipes[outPipe_Id],*function);
			relop=T;
	}



	void GroupNode::PrintNode(){
   		cout<<"*****************************************"<<endl;

   		cout<<opName<<endl;

   			cout<<"Input Pipe:"<<leftPipe_Id<<endl;
   		cout<<"OutPut pipe ID :"<<outPipe_Id<<endl;
   		cout<<"Output Schema :"<<endl;
   		schema->PrintSchema();
   		cout<<endl;

		cout<<opName<<"  Function"<<endl;
		function->Print();
		cout<<endl;

		cout<<opName<<"  OrderMaker"<<endl;
		order->Print();
		cout<<endl;
	}

	void  GroupNode:: Execute(){
			GroupBy *G=new GroupBy;
			G->Use_n_Pages(RUNLEN);
			G->Run(*pipes[leftPipe_Id],*pipes[outPipe_Id],*order,*function);
			relop=G;
			}


	void ProjectNode::PrintNode(){
   		cout<<"*****************************************"<<endl;

   		cout<<opName<<endl;

   			cout<<"Input Pipe:"<<leftPipe_Id<<endl;
   			cout<<"OutPut pipe ID :"<<outPipe_Id<<endl;
   		cout<<"Output Schema :"<<endl;
   		schema->PrintSchema();
   		cout<<endl;
		cout<<"Num to keep"<<numAttsOut<<endl;
		cout<<opName<<"  Attributes to Keep"<<endl;
		for(int i=0;i<numAttsOut;i++){
			cout<<attsToKeep[i]<<"\t";
		}
		cout<<endl;
	}

	void ProjectNode::Execute(){
		Project *P=new Project;
		P->Use_n_Pages(RUNLEN);
		P->Run(*pipes[leftPipe_Id],*pipes[outPipe_Id],attsToKeep,numAttsIn,numAttsOut);
		relop=P;

	}


	void DistinctNode:: PrintNode(){
   		cout<<"*****************************************"<<endl;

   		cout<<opName<<endl;

   		cout<<"Input Pipe:"<<leftPipe_Id<<endl;
   		cout<<"OutPut pipe ID :"<<outPipe_Id<<endl;
   		cout<<"Output Schema :"<<endl;
   		schema->PrintSchema();
   		cout<<endl;
	}

	void DistinctNode::Execute(){
		DuplicateRemoval *D= new DuplicateRemoval;
		D->Use_n_Pages(10);
		D->Run(*pipes[leftPipe_Id],*pipes[outPipe_Id],*schema);
		relop=D;
	}


	void   WriteOutNode::PrintNode(){
		cout<<"*****************************************"<<endl;

		cout<<opName<<endl;

		cout<<"Input Pipe:"<<leftPipe_Id<<endl;
		cout<<"Output Schema :"<<endl;
		schema->PrintSchema();
		cout<<"Output File : "<<outFile<<endl;
		cout<<endl;

	}
	void WriteOutNode:: Execute(){
		WriteOut *W=new WriteOut;
		relop=W;
		W->Use_n_Pages(10);
		FILE *writefile = fopen (outFile, "w");
		W->Run(*pipes[leftPipe_Id],writefile,*schema);

	}
