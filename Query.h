#ifndef QUERY_H_
#define QUERY_H_
#include <algorithm>
#include <iostream>
#include "Statistics.h"
#include "ParseTree.h"
#include "Nodes.h"
#include <set>
#include <vector>
#include "Defs.h"
#include <string>
#include "limits.h"
#include "Pipe.h"
#include "Schema.h"
#define p(EX) cerr << #EX << ": " << (EX) << endl;





class Query{
public:
	Statistics s;
	typedef struct{
		AndList *and_list;
		char **rel_names;
		int num_tables;
		set<string> table_set;
	}Operation;

	typedef struct{
		long double cost;
		string join_first;
		string join_second;
		Statistics s;
		AndList *and_list;
	}DP;

	map<string, AndList* >sel_hash;
	map<string, Operation* > join_hash;
	map<string, DP*> dp_hash;
	map<string, AndList* >:: iterator it_sel_hash;
	map<string, Operation* >:: iterator it_join_hash;
	map<string, DP* >:: iterator it_dp_hash;
	map<string, Schema*> schema_hash;
	map<string, Schema*> :: iterator it_schema_hash;
	map<string,char*> rel_map;
	set<string> table_set;
	set<string>::iterator it_table_set;
	vector<string> vec_tables;
	Node* join_root;
	string table_name;
	vector <Node*> node_list;
	bool writeFile;
	int nodecount;

	Query();
	void printNode(Node* );
	void printDPHash();
	void printselHash();
	void initPipes(int n);
	int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) ;
	Node* createSelectJoinTree(string relnames);
	void loadNode(Node *lnode);
	void mergeAndlist(AndList *left, AndList *right);
	AndList* getcnf(vector <string> left, string right);
	long double calculate(vector<string> vec, string name);
	void parseTables();
	void parseWhereClause();
	void fillDPtwoRelationJoin();
	Node* createFinalTree();
	void runQuery();
	Node* execute();
	void run();
	void showPlan();

};

#endif /* QUERY_H_ */
