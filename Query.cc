#include "Query.h"
using namespace std;

extern "C" struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern "C" struct TableList *tables; // the list of tables and aliases in the query
extern "C" struct AndList *boolean; // the predicate in the WHERE clause
extern "C" struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern "C" struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern "C" int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern "C" int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern "C" struct Table *tb;
extern "C" struct AttList *alist;
extern "C" char *myfile;
extern "C" char *droptable;
extern "C" char *mytable;
extern "C" char* catalog_path;
extern "C" char* outFile;

Pipe *pipes[20];

void printAndList(AndList* boolean){
	cerr<<"Printing and list\n";
	AndList *andtemp, *temp;
		andtemp=boolean;
		OrList *ortemp;
		while(andtemp){
				 ortemp=andtemp->left;
				while(ortemp){
						cerr<<ortemp->left->left->value<<" ";
						cerr<<ortemp->left->code<<" ";
						cerr<<ortemp->left->right->value<<endl;
						ortemp=ortemp->rightOr;
					}
					cerr<<endl;
				andtemp=andtemp->rightAnd;
			}
}


Query:: Query(){
	nodecount=-1;
	writeFile=false;

}



void Query::printNode(Node *pNode){
	if(pNode!=NULL){
		printNode(pNode->left);
		printNode(pNode->right);
		pNode->PrintNode();
	}
}


void Query::printDPHash(){
	cerr<<"Printng DP hash\n";
	for(it_dp_hash=dp_hash.begin();it_dp_hash!=dp_hash.end();it_dp_hash++){
		p(it_dp_hash->first)
				cerr<<it_dp_hash->second->join_first<<":";
				cerr<<it_dp_hash->second->join_second<<":";
				cerr<<it_dp_hash->second->cost<<endl;
	}
}

void Query::printselHash(){
	cerr<<"Printng sel hash\n";
	for(it_sel_hash=sel_hash.begin();it_sel_hash!=sel_hash.end();it_sel_hash++){
		p(it_sel_hash->first);

	}
}



void Query::initPipes(int n){
	for(int i=0;i<n;i++){
				pipes[i]=new Pipe(100);
	}
}


int Query::clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		if(cnt%1000==0)
			cerr<<".";
		cnt++;
	}
	return cnt;
}



Node* Query::createSelectJoinTree(string relnames){

       Node *rootnode;

       if(dp_hash.find(relnames)!=dp_hash.end()){
    	   	   JoinNode *node=new JoinNode;
    	   	   node->opName= "Join";
               node->opType=join;
               node->left=createSelectJoinTree(dp_hash.find(relnames)->second->join_first);
               node->right=createSelectJoinTree(dp_hash.find(relnames)->second->join_second);
               node->leftPipe_Id=node->left->outPipe_Id;
               node->rightPipe_Id=node->right->outPipe_Id;
               node->outPipe_Id=++nodecount;
               node->schema=new Schema(*(node->left->schema),*(node->right->schema));
               CNF *mycnf=new CNF;
               Record *literal=new Record;
                //printAndList(dp_hash.find(relnames)->second->and_list);

                mycnf->GrowFromParseTree(dp_hash.find(relnames)->second->and_list,node->left->schema,node->right->schema,*literal);
                node->literal=literal;
                node->cnf=mycnf;
                rootnode=node;
       }
       else if(sel_hash.find(relnames)!=sel_hash.end()){


    	   	   SelectNode *node = new SelectNode;
    	   	   node->opName="Select File";
               node->opType=selectF;
               node->outPipe_Id=++nodecount;
               node->left=NULL;
               node->right=NULL;
               node->schema=new Schema(*(schema_hash.find(relnames)->second));
               node->relName=rel_map.find(relnames)->second;
               CNF *mycnf=new CNF;
               Record *literal=new Record;
               mycnf->GrowFromParseTree(sel_hash.find(relnames)->second,node->schema,*literal);
               node->cnf=mycnf;
               rootnode=node;
               node->literal=literal;
       }
       else{
    	   	   CNF*cnf=new CNF;
    	   	   Record *literal=new Record;
    	   	   SelectNode* node=new SelectNode;
    	   	   node->opName="Select File";
               node->opType=selectF;
               node->left=NULL;
               node->right=NULL;
               node->outPipe_Id=++nodecount;
               node->schema=new Schema(*(schema_hash.find(relnames)->second));
               cnf->GrowFromParseTree(NULL,node->schema,*literal);
               node->cnf=cnf;
               node->literal=literal;
               node->relName=rel_map.find(relnames)->second;
               rootnode=node;
       }

       return rootnode;
}





void Query::loadNode(Node *lnode){
	if(lnode!=NULL){
			loadNode(lnode->left);
			loadNode(lnode->right);
			node_list.push_back(lnode);
	}
}


void Query::mergeAndlist(AndList *left, AndList *right){


	AndList* temp=left;
	while(temp->rightAnd){
		temp=temp->rightAnd;
	}
	//now left has NULL, join here
	temp->rightAnd=right;
}




AndList* Query::getcnf(vector <string> left, string right){
	AndList *temp=NULL;
	AndList *andlist=temp;
	for(it_join_hash=join_hash.begin();it_join_hash!=join_hash.end();it_join_hash++){

		set<string>::iterator it_set;
		if(it_join_hash->second->table_set.find(right)!=it_join_hash->second->table_set.end()){
			for(int i=0;i<left.size();i++){

				if(it_join_hash->second->table_set.find(left.at(i))!=it_join_hash->second->table_set.end()){

					AndList*  temp_loc=it_join_hash->second->and_list;

					while(temp_loc!=NULL){
						if(temp==NULL){
						temp=new AndList;
						andlist=temp;
						}
						else{
							temp->rightAnd=new AndList;
							temp=temp->rightAnd;
						}
						temp->left=temp_loc->left;
						temp->rightAnd=NULL;
						temp_loc=temp_loc->rightAnd;
					}
					break;
				}
			}
		}
	}
	return andlist;
}

long double Query::calculate(vector<string> vec, string name){

	if(dp_hash.find(name)!=dp_hash.end()){ 		//return the result if it already exists in the map
		return dp_hash.find(name)->second->cost;
	}
	else{
		//c,l,o,r
		long double min=-1;
		DP *dp= new DP;
		vector<string> minvec;
		char **relNames=new char*[10];
		int relCount=0;
		for(int i=0;i<vec.size();i++){
			//for c
			string left="";

			vector<string> passvec;
			string right=vec.at(i);
			relNames[relCount++]=(char*)right.c_str();

			for(int j=0;j<vec.size();j++){
					if(i==j)
						continue;
					left.append(vec.at(j));
					 left.append(",");
					passvec.push_back(vec.at(j));

			}

			//left has lor
			//right has c
			long double cost = calculate(passvec,left);

			if(min==-1 || cost<=min){
				min=cost;
				dp->join_first=left;
				dp->join_second=right;
				minvec=passvec;
			}

		}//end of i




		//find and merge all possible cnfs of right with each individual of left

		Statistics new_s(dp_hash.find(dp->join_first)->second->s);


		AndList *toApply = getcnf(minvec,dp->join_second);

		dp->and_list=toApply;
		dp->cost=new_s.Estimate(toApply,relNames,relCount)+min;

		new_s.Apply(toApply,relNames,relCount);
		dp->s=new_s;

		dp_hash[name]=dp;

		return dp->cost;
	}
}
//populates vec_tables
void Query::parseTables(){
	while(tables){

		Schema *oldSchema=new Schema(catalog_path,tables->tableName);
		 schema_hash[tables->tableName]=oldSchema;
		if(tables->aliasAs!=NULL){
					s.CopyRel(tables->tableName,tables->aliasAs);
					vec_tables.push_back(tables->aliasAs);
					Schema *newSchema=new Schema(*oldSchema, tables->aliasAs);
				   	schema_hash[tables->aliasAs]=newSchema;
				   	rel_map[tables->aliasAs]=tables->tableName;
		}
		else{
					vec_tables.push_back(tables->tableName);
		}

		tables=tables->next;
	}
	sort(vec_tables.begin(),vec_tables.end());
}

void Query::parseWhereClause(){

	map<string,pair <int,string >  > ::iterator it_att;
	AndList *andtemp, *temp;
	andtemp=boolean;
	OrList *ortemp;
	while(andtemp){
			set<string> myset; //contains the list of tables in the andlist
			set<string>::iterator it_set;
			 ortemp=andtemp->left;

			while(ortemp){

					if(ortemp->left->left->code==3){
						it_att=s.att_hash.find(ortemp->left->left->value);
						myset.insert(it_att->second.second);
					}
					if(ortemp->left->right->code==3){
						it_att=s.att_hash.find(ortemp->left->right->value);
						myset.insert(it_att->second.second);
					}
					ortemp=ortemp->rightOr;
				} // end of inner while
				temp=andtemp;
				andtemp=andtemp->rightAnd;
				temp->rightAnd=0;
				Operation *opt=new Operation;
				opt->num_tables=myset.size();
				opt->rel_names = new char*[10];
				opt->and_list=temp;
				string name="";
				if(myset.size()>0){
					int count=0;
					if(myset.size()>1){	//2 or more table in AndList
						for(it_set=myset.begin();it_set!=myset.end();it_set++){
									opt->rel_names[count++]=(char *)((*it_set).c_str());
									name.append(*it_set);
									 name.append(",");
						}
						opt->table_set=myset;
						it_join_hash=join_hash.find(name);
						if(it_join_hash==join_hash.end()){ //first time
							join_hash[name]=opt;
						}
						else{		//second or there after
							mergeAndlist(it_join_hash->second->and_list,opt->and_list);
						}
					}
					else{	//Select Condition
						name.append(*myset.begin());
						it_sel_hash=sel_hash.find(name);
						if(it_sel_hash==sel_hash.end()){
							sel_hash[name]=opt->and_list;
						}
						else{
							mergeAndlist(it_sel_hash->second,opt->and_list);
						}
						opt->rel_names[0]=(char *)name.c_str();
						s.Apply(opt->and_list,opt->rel_names,opt->num_tables);
					}
				}
				else{
					p("Handle 5 > 12 case");
				}
		} // end of outer while.Parsing of where clause done
}

void Query::fillDPtwoRelationJoin(){
	for(int i=0;i<vec_tables.size()-1;i++){
		table_name.append(vec_tables.at(i));
		 table_name.append(",");

		 for(int j=i+1;j<vec_tables.size();j++){
			string comb_2=vec_tables.at(i);
			 comb_2.append(",");
			comb_2.append(vec_tables.at(j));
			comb_2.append(",");

			DP *dp = new DP;
			dp->join_first=vec_tables.at(i);
			dp->join_second=vec_tables.at(j);

			char ** dp_rel_names=new char*[2];
			dp_rel_names[0]=(char *)dp->join_first.c_str();
			dp_rel_names[1]=(char *)dp->join_second.c_str();

			Statistics new_s(s);

			it_join_hash=join_hash.find(comb_2); //find if this join already exists

			if(it_join_hash==join_hash.end()){	//if the combination was not found in rel hash
						long double l = s.rel_hash.find(dp->join_first)->second.first;
						long double r = s.rel_hash.find(dp->join_second)->second.first;
						dp->and_list=0;
						dp->cost=l*r;
			}
			else{
				dp->and_list=it_join_hash->second->and_list;
				dp->cost=s.Estimate(dp->and_list,dp_rel_names,2);
			}
			new_s.Apply(dp->and_list,dp_rel_names,2);
			dp->s=new_s;
			dp_hash[comb_2]=dp;
		}
	}
}


Node* Query::createFinalTree(){

	GroupNode * grpNode;
	SumNode *sumNode;
	Function *func=new Function;
	FuncOperator *t_f=finalFunction;
	NameList *temp_select=attsToSelect;
	ProjectNode *projNode;
	int projCnt=-1;

	while(temp_select){
		projCnt++;
		temp_select=temp_select->next;
	}
	temp_select=attsToSelect;
	int gCnt=-1;
	int tmpCnt=-1;
	if(finalFunction){
		func->GrowFromParseTree(finalFunction,*(join_root->schema));
		//func.Print();

		NameList* temp_gAtts=groupingAtts;

		if(temp_gAtts){
			OrderMaker *orderMaker = new OrderMaker;
			Schema *g_schema=new Schema;

			while(temp_gAtts){
				//cerr<<temp_gAtts->name<<",";
				temp_gAtts=temp_gAtts->next;
				gCnt++;
			}

			tmpCnt=gCnt;
			if(gCnt==projCnt){
				temp_gAtts=temp_select;
			}
			else
				temp_gAtts=groupingAtts;

			orderMaker->numAtts=gCnt+1;

			g_schema->myAtts=new Attribute[gCnt+2];
			g_schema->numAtts=gCnt+2;

			g_schema->myAtts[0].name="sum";
			g_schema->myAtts[0].myType= Type(1-func->returnsInt);

			while(temp_gAtts){
						for(int i =0;i<join_root->schema->numAtts;i++){
							if(strcmp(join_root->schema->myAtts[i].name,temp_gAtts->name)==0){
									g_schema->myAtts[gCnt+1].name=strdup(join_root->schema->myAtts[i].name);
									g_schema->myAtts[gCnt+1].myType=join_root->schema->myAtts[i].myType;
									orderMaker->whichAtts[gCnt]=i;
									orderMaker->whichTypes[gCnt--]=join_root->schema->myAtts[i].myType;
							}
						}
						temp_gAtts=temp_gAtts->next;
			}

			grpNode=new GroupNode;
			grpNode->left=join_root;
			grpNode->right=NULL;
			grpNode->leftPipe_Id=join_root->outPipe_Id;
			grpNode->outPipe_Id=++nodecount;
			grpNode->opName="Group By";
			grpNode->opType=groupBy;
			grpNode->order=orderMaker;
			grpNode->function=func;
			grpNode->schema=g_schema;
			join_root=grpNode;
		}

		else{

			sumNode=new SumNode;
			sumNode->left=join_root;
			sumNode->right=NULL;
			sumNode->function=func;
			sumNode->leftPipe_Id=join_root->outPipe_Id;
			sumNode->opName="Sum";
			sumNode->opType=sum;
			sumNode->outPipe_Id=++nodecount;

			Schema  *sum_schema=new Schema;
			sum_schema->numAtts=1;
			sum_schema->myAtts=new Attribute[1];
			sum_schema->myAtts[0].name="sum";
			sum_schema->myAtts[0].myType=Type(1-func->returnsInt);
			sumNode->schema=sum_schema;
			join_root=sumNode;
		}
}



	if(projCnt!=-1){				//We have to do a a project

		Schema *proj_schema;


		if(!groupingAtts){			// there is no group by to handle
			projNode=new ProjectNode;
			temp_select=attsToSelect;
				proj_schema=new Schema;

				proj_schema->myAtts=new Attribute[projCnt+1];
				proj_schema->numAtts=projCnt+1;
				projNode->attsToKeep=new int[projCnt+1];
				projNode->numAttsOut=projCnt+1;
				int j=projCnt;

				while(temp_select){
					for(int i =0;i<join_root->schema->numAtts;i++){
							if(strcmp(join_root->schema->myAtts[i].name,temp_select->name)==0){
								projNode->attsToKeep[j--]=i;

								proj_schema->myAtts[projCnt].name=strdup(join_root->schema->myAtts[i].name);
								proj_schema->myAtts[projCnt--].myType=join_root->schema->myAtts[i].myType;
									break;
							}
					}
					temp_select=temp_select->next;
				}

						projNode->left=join_root;
						projNode->right=NULL;
						projNode->leftPipe_Id=join_root->outPipe_Id;
						projNode->outPipe_Id=++nodecount;
						projNode->opName="Project";
						projNode->opType=project;

						projNode->schema=proj_schema;
						join_root=projNode;

		}

		else if(projCnt<tmpCnt){

							projNode=new ProjectNode;
							temp_select=attsToSelect;
							proj_schema=new Schema;
							proj_schema->myAtts=new Attribute[projCnt+2];
							proj_schema->numAtts=projCnt+2;

							proj_schema->myAtts[0].name="sum";
							proj_schema->myAtts[0].myType= Type(1-func->returnsInt);
							projNode->attsToKeep=new int[projCnt+2];
							projNode->numAttsOut=projCnt+2;
							int j=projCnt+1;
							while(temp_select){
										for(int i =0;i<join_root->schema->numAtts;i++){
											if(strcmp(join_root->schema->myAtts[i].name,temp_select->name)==0){
													projNode->attsToKeep[j--]=i;
													proj_schema->myAtts[projCnt+1].name=strdup(join_root->schema->myAtts[i].name);
													proj_schema->myAtts[projCnt+1].myType=join_root->schema->myAtts[i].myType;
													projCnt--;
													break;
											}
										}
										temp_select=temp_select->next;
							}
									projNode->left=join_root;
									projNode->right=NULL;
									projNode->leftPipe_Id=join_root->outPipe_Id;
									projNode->outPipe_Id=++nodecount;
									projNode->opName="Project";
									projNode->opType=project;
									projNode->schema=proj_schema;
									//projNode->schema->PrintSchema();
									join_root=projNode;

						}
		else{

		}

	}
	if(distinctAtts || distinctFunc){

		DistinctNode *distinctNode=new DistinctNode;
		distinctNode->opName="Distinct";
		distinctNode->opType=distinct;
		distinctNode->left=join_root;
		distinctNode->right=NULL;
		distinctNode->leftPipe_Id=join_root->outPipe_Id;
		distinctNode->outPipe_Id=++nodecount;
		distinctNode->schema=new Schema(*(join_root->schema));
		//distinctNode->schema->PrintSchema();

		join_root=distinctNode;
	}
	if(writeFile){

		WriteOutNode *writeOutNode = new WriteOutNode;
		writeOutNode->opName="Write Out";
		writeOutNode->schema=new Schema(*(join_root->schema));
		writeOutNode->outFile=strdup(outFile);
		writeOutNode->leftPipe_Id=join_root->outPipe_Id;
		writeOutNode->left=join_root;
		writeOutNode->right=NULL;
		writeOutNode->outPipe_Id=-1;

		join_root=writeOutNode;
	}


	return join_root;
}


void Query::runQuery(){

	initPipes(20);

	loadNode(join_root);


	for(int i =0;i<node_list.size();i++){
		Node *newnode= node_list.at(i);
		newnode->Execute();
	}


	Node * t_node=node_list.at(node_list.size()-1);
	if(t_node->outPipe_Id>=0){

			int cnt=clear_pipe (*pipes[t_node->outPipe_Id], t_node->schema,true);
			cerr<<"Rows :"<<cnt<<endl;
	}
	for(int i =0;i<node_list.size();i++){
		Node *newnode= node_list.at(i);
		newnode->relop->WaitUntilDone();
	}

}



Node* Query ::  execute(){
	s.Read("references.txt");
	parseTables();

	parseWhereClause();

	fillDPtwoRelationJoin();

	table_name.append(vec_tables.at(vec_tables.size()-1));

	if(vec_tables.size()>1)
		table_name.append(",");

	if(vec_tables.size()>1)
		calculate(vec_tables,table_name);
	join_root=createSelectJoinTree(table_name);
	join_root=createFinalTree();
	return join_root;
}

void Query::showPlan(){
	Node *root=execute();
	printNode(root);
};

void Query::run(){
	execute();

	runQuery();
}
