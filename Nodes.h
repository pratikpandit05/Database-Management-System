#ifndef NODES_H_
#define NODES_H_


#include <cstring>
#include "Defs.h"
#include <stdio.h>
#include <iostream>
#include "Schema.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include "Function.h"
#include "RelOp.h"
#include "DBFile.h"
#define RUNLEN 1000

using namespace std;

class Node{

public:
	   string opName;
	   OpType opType;
	   int leftPipe_Id;
	   Schema *schema;
	   Node* left;
	   Node *right;
	   int outPipe_Id;
	   RelationalOp *relop;
	   Pipe* leftPipe;
	   Pipe* rightPipe;
	   Pipe* outPipe;

	   virtual void PrintNode(){}
	   virtual void Execute(){}
	   virtual void End(){}
	   virtual int clear_pipe(bool p){}


};


//Node for select file
class SelectNode : public Node{
public:
	Record *literal;
	CNF *cnf;
	char* relName;
	DBFile *dbf;
	SelectFile *sf;

	void PrintNode();
	 void Execute();
	 int clear_pipe (bool print);

};


class JoinNode : public Node{
public:
	CNF *cnf;
	Record *literal;
	int rightPipe_Id;

	void PrintNode();
	void Execute();
};


class SumNode : public Node {
public:
	Function *function;
	void PrintNode();
	void Execute();
};


class GroupNode : public Node{
public:
	OrderMaker *order;
	Function *function;
	void PrintNode();
	void Execute();

};

class ProjectNode : public Node{
public:
	int *attsToKeep;
	int numAttsOut;
	int numAttsIn;
	void PrintNode();
	void Execute();
};

class DistinctNode : public Node{
public:
	void PrintNode();
	void Execute();
};

class WriteOutNode : public Node{
public:
	char *outFile;
	void PrintNode();
	void Execute();
};

#endif /* NODES_H_ */
