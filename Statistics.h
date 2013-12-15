#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <algorithm>
#include <vector>
#include <set>
#include <math.h>

using namespace std;

class Statistics
{
public:
	map<string,pair <int,string >  > att_hash;
	map<string,pair<int,vector<string> > > rel_hash;
	map<string,pair<int,vector<string> > >::iterator it;
	//map<string,pair<int,map<string,int> > > rel_hash;

	//map<string,pair<int,map<string,int> > >::iterator it;
	vector<string> joined;
	 map<string, int> pointer;


	 map<string,int> ::iterator internal_it;
	 map<string,pair <int,string >  > ::iterator it_att_hash;
	 map<string,int>::iterator it_pointer;

	int joinCount;
	void UpdateAtt(char *attName, int numDistincts);
	int GetDistinct(char* name);
	int GetTuples(string name);
	void PrintAttHash();
	void PrintPointer();
	void PrintJoined();




public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	long double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	void WritePointer(char* fromWhere);
	void WriteJoined(char*fromWhere);
	void ReadPointer(char* fromWhere);
	void ReadJoined(char* fromWhere);
};

#endif
