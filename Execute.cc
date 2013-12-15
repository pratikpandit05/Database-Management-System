#include "y.tab.h"
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
#include "Schema.h"
#include "Defs.h"
#include "Pipe.h"
#define p(EX) cerr << #EX << ": " << (EX) << endl;
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;
char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";

DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c,dbf_n,dbf_r;

using namespace std;

class Node{
public:
	string opName;
	Record literal;
	char *relation;
	int numAttsIn;
	int numAttsOut;
	OpType opType;
	int leftPipe_Id;
	int rightPipe_Id;
	int outPipe_Id;
	int *attsToKeep;
	Function *func;
	Node* left;
	Node* right;
	Schema *schema;
	OrderMaker *order;
	CNF cnf;
	FILE *outFile;
};

vector<Node*> list;

int main(){

	Pipe *pipes[]= new Pipe[list.size];
	for(int i=0;i<list.size();i++){
		pipes[i]=new Pipe;
	}



	for(int i=0;i<list.size();i++){
		switch(list.at(i)->opType)
			case selectF:
				char rpath[100];
				sprintf (rpath, "%s.bin", list.at(i)->rname);
					SelectFile sf;
					DBFile dbf;
					dbf.Open(rpath);
					sf.Use_n_Pages(100);
					sf.Run(dbf,pipes[list.at(i)->leftPipe_Id],list.at(i)->cnf,list.at(i)->literal);


				break;
			case selectP:
				char rpath[100];
				sprintf (rpath, "%s.bin", list.at(i)->rname);
				SelectFile sf;
				DBFile dbf;
				dbf.Open(rpath);
				sf.Use_n_Pages(100);
				sf.Run(dbf,pipes[list.at(i)->leftPipe_Id],*(list.at(i)->cnf),*(list.at(i)->literal));
				break;
			case join:
				Join j;
				j.Use_n_Pages(100);
				j.Run(pipes[list.at(i)->leftPipe_Id],pipes[list.at(i)->rightPipe_Id],pipes[list.at(i)->outPipe_Id],*(list.at(i)->cnf),*(list.at(i)->literal));
				break;
			case groupBy:
				GroupBy G;
				G.Use_n_Pages(100);
				G.Run(pipes[list.at(i)->leftPipe_Id],pipes[list.at(i)->outPipe_Id],*(list.at(i)->order),*(list.at(i)->func));

				break;
			case project:
				Project P;
				P.Use_n_Pages(100);
				P.Run(pipes[list.at(i)->leftPipe_Id],pipes[list.at(i)->outPipe_Id],list.at(i)->keepMe,list.at(i)->numAttsIn,list.at(i)->numAttsOut);
				break;
			case sum:
				Sum T;
				T.Use_n_Pages(1);
				T.Run(pipes[list.at(i)->leftPipe_Id],pipes[list.at(i)->outPipe_Id],*(list.at(i)->func));
				break;
			case distinct:
				DuplicateRemoval D;
				D.Use_n_Pages(10);
				D.Run(pipes[list.at(i)->leftPipe_Id],pipes[list.at(i)->outPipe_Id],*(list.at(i)->schema));
				break;
			case writeOut:
				WriteOut W;
				W.Use_n_Pages(1);
				W.Run(pipes[list.at(i)->leftPipe_Id],list.at(i)->outFile,*(list.at(i)->schema));
				break;
	}
		RelationalOp r;
		r.

	clear_pipe()

}
