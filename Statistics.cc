#include "Statistics.h"
#include <map>
#include "Defs.h"
#include <list>
#define p(EX) cerr << #EX << ": " << (EX) << endl;

Statistics::Statistics()
{
	rel_hash.clear();
	joinCount=0;
}
Statistics::Statistics(Statistics &copyMe){


	int num_tuples;
	vector<string> att_vec;
	vector<string> attributes;
	for(copyMe.it=copyMe.rel_hash.begin();copyMe.it!=copyMe.rel_hash.end();copyMe.it++){

		pair<int,vector<string> >  value;
		value.first=copyMe.it->second.first;
		att_vec=copyMe.it->second.second;
		attributes.clear();
		attributes.insert(attributes.end(),copyMe.it->second.second.begin(),copyMe.it->second.second.end());

		for(int i=0;i<att_vec.size();i++){
				//attributes.push_back(att_vec.at(i));

				copyMe.it_att_hash= copyMe.att_hash.find(att_vec.at(i));
				pair<int, string > internal_pair;
				internal_pair.first=copyMe.it_att_hash->second.first;
				internal_pair.second=copyMe.it->first;
				//att_hash.insert(pair <string,pair<int, string > > (  att_vec.at(i) ,  internal_pair));
				att_hash[att_vec.at(i)]=internal_pair;
		}

		value.second=attributes;
		rel_hash[copyMe.it->first]=value;
		pointer[copyMe.it->first]=-1;


	}

	joinCount=copyMe.joinCount;
	joined.clear();
	joined.insert(joined.end(),copyMe.joined.begin(),copyMe.joined.end());

	for(copyMe.it_pointer=copyMe.pointer.begin();copyMe.it_pointer!=copyMe.pointer.end();copyMe.it_pointer++){
		//pointer.insert(pair<string,int> (copyMe.it_pointer->first,copyMe.it_pointer->second));
		pointer[copyMe.it_pointer->first]=copyMe.it_pointer->second;
	}

}
Statistics::~Statistics()
{
/*
	for(it=rel_hash.begin();it!=rel_hash.end();it++){
	char *temp=it->first;
	delete temp;

	}
	rel_hash.clear();
*/
}

void Statistics::AddRel(char *relName, int numTuples)
{
		vector<string>attributes;
		pair<int,vector<string> >  value;
		value.first=numTuples;
		value.second=attributes;

		rel_hash[relName]=value;
		pointer[relName]=-1;

}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{


	it=rel_hash.find(relName);

	if(it!=rel_hash.end()){

		vector<string> att_vec=it->second.second;

			it->second.second.push_back(string(attName));
			pair<int, string > internal_pair;
			if(numDistincts==-1)
				numDistincts=it->second.first;
			internal_pair.first=numDistincts;
			internal_pair.second=relName;
			//att_hash.insert(pair <string,pair<int, string > > (  attName,  internal_pair));
			att_hash[attName]=internal_pair;
	}

	else{
		cerr<<"Error : Relation does not exists in AddAtt"<<endl;
		exit(1);
	}

}
void Statistics::CopyRel(char *oldName, char *newName)
{



	it=rel_hash.find(oldName);

	if(it!=rel_hash.end()){

		vector<string> attributes;
		pair<int,vector<string>  > value;
		value.first=it->second.first;
		vector<string> att_vec=it->second.second;
		//map<string,pair<int,vector<string  > > >::iterator new_it;
		//new_it=rel_hash.find(newName);


		string newAttName;
		for(int i=0;i<att_vec.size();i++){

			newAttName=newName;
			newAttName.append(".");
			newAttName.append(att_vec.at(i));

			//new_it->second.second.push_back(newAttName);
			attributes.push_back(newAttName);
			it_att_hash= att_hash.find(att_vec.at(i));
			pair<int, string > internal_pair;
			internal_pair.first=it_att_hash->second.first;
			internal_pair.second=newName;
			//att_hash.insert(pair <string,pair<int, string > > (  newAttName ,  internal_pair));
			att_hash[newAttName]=internal_pair;
		}

		value.second=attributes;

		rel_hash[newName]=value;
		pointer[newName]=-1;



	}

	else{

		cerr<<"Error. Relation  to copy "<<oldName<<" does not exist"<<endl;
		exit(1);
	}



}
	
void Statistics::Read(char *fromWhere)
{
	//TO-DO
	rel_hash.clear();
	att_hash.clear();
	string line;
	char *lstr;
	char *fstr;
	char *sstr;
	char *tstr;

	ifstream myfile (fromWhere);

	if (myfile.is_open()){
		    while ( !myfile.eof() ){

		    	getline (myfile,line);

		    	if(line.size()<=0)
		    		break;

		    	lstr = new char [line.size()+1];
				strcpy (lstr, line.c_str());


				 fstr=strtok (lstr,":");
				 sstr=strtok(NULL,":");
				 tstr=strtok(NULL,"	 ");

				 if(tstr==NULL){

					 AddRel(fstr,atoi(sstr));
				 }
				 else{
					 //insert attributes
					 AddAtt(fstr,sstr,atoi(tstr));

			 	}
				 delete[] lstr;

				 }

		    }
		myfile.close();
		//ReadJoined("Joined.txt");
		//ReadPointer("Pointers.txt");
	}



void Statistics::Write(char *fromWhere)
{

	ofstream stats_file;
	stats_file.open(fromWhere);
	vector<string> att_vec;

	for(it=rel_hash.begin();it!=rel_hash.end();it++){
		stats_file<<it->first<<":"<<it->second.first<<endl;

		att_vec=it->second.second;
		for(int i=0;i<att_vec.size();i++){
			it_att_hash=att_hash.find(att_vec.at(i));
			stats_file<<it->first<<":"<<it_att_hash->first<<":"<<it_att_hash->second.first<<endl;
		}
	}
	stats_file.close();
	//WriteJoined("Joined.txt");
	//WritePointer("Pointers.txt");


}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
			long double total=1.0;
			long double relFactor=1.0;
			long double tuples=0.0;
			long double min;
			set<string> myset;
			set<string>::iterator it_set;
			map<string,int>::iterator it_pointer;
			char* token;
			double dist_l=0.0;
			double dist_r=0.0;
			double andFactor=1.0;
			map<string,double> or_eval_map;
			map<string,double>::iterator it_or_eval_map;

//--------------------------------------------------------------------------------------------------------------Estimate andFactor--------------------------------------------------------------------------------------------------------------------------
			for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
				if (parseTree == NULL) {
					break;
				}
				struct OrList *myOr = parseTree->left;
				double orFactor=1.0;
				or_eval_map.clear();
				for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
					if (myOr == NULL) {
						break;
					}
					if(myOr->left->code == EQUALS){
						if(myOr->left->left->code == NAME && myOr->left->right->code == NAME){
							dist_l=GetDistinct(myOr->left->left->value);
							dist_r= GetDistinct(myOr->left->right->value);
							min = dist_l < dist_r ? dist_l : dist_r;
							if(min!=dist_l)
								UpdateAtt(myOr->left->left->value,min);
							else
								UpdateAtt(myOr->left->right->value,min);
							orFactor*=1-1/((dist_l>dist_r)?dist_l:dist_r);
						}
						else if (myOr->left->left->code == NAME) {
							dist_l=GetDistinct(myOr->left->left->value);
							//orFactor*=1-1/dist_l;
							it_or_eval_map=or_eval_map.find(myOr->left->left->value);
							if(it_or_eval_map==or_eval_map.end())
								or_eval_map[myOr->left->left->value]=1.0/dist_l;
							else
								or_eval_map[myOr->left->left->value]+=1.0/dist_l;
						}
						else if (myOr->left->right->code == NAME) {
							dist_r=GetDistinct(myOr->left->right->value);
							//orFactor*=1-1/dist_r;
							if(it_or_eval_map==or_eval_map.end())
									or_eval_map[myOr->left->left->value]=1.0/dist_r;
							else
									or_eval_map[myOr->left->left->value]+=1.0/dist_r;
						}
						else{
							cerr<<"Error : Not a valid CNF"<<endl;
							exit(1);
						}
					}
					//not equals
					else{
							if(myOr->left->right->code == NAME && att_hash.find(myOr->left->right->value)==att_hash.end() || myOr->left->left->code == NAME && att_hash.find(myOr->left->left->value)==att_hash.end()){
								cerr<<"Error : Not a valid CNF"<<endl;
								exit(1);
							}
								orFactor=orFactor*2.0/3.0;
					}
				}//or loop ends
				for(it_or_eval_map=or_eval_map.begin();it_or_eval_map!=or_eval_map.end();it_or_eval_map++){
					orFactor*=(1-(it_or_eval_map->second >1?1:it_or_eval_map->second));
				}
				andFactor*=(1-orFactor);
			}
//---------------------------------------------------------------------------Estimate and Join relations-------------------------------------------------------------------------------------------------
			for(int i=0;i<numToJoin;i++){
					myset.insert(relNames[i]);
			}
			string joined_string;
			vector<string> joined_attrs;
			while(myset.size()>0){
				string front=*(myset.begin());
				it_pointer=pointer.find(front);
				if(it_pointer==pointer.end()){
								cerr<<"Relation in relNames does not exist in Statistics"<<endl;
								exit(-1);
				}
				else if(it_pointer->second==-1){ //relation exists singly
					tuples= GetTuples(front);
					relFactor=relFactor*tuples;
					joined_string.append(front);
					joined_string.push_back('|');
					it_pointer->second=joinCount;
					it=rel_hash.find(front);
					joined_attrs.insert(joined_attrs.end(),it->second.second.begin(),it->second.second.end());
					//update the relation pointers of each attr.
					myset.erase(myset.begin());
					rel_hash.erase(it);
				}
				else{//relation has been joined
					string joined_rel;
					joined_rel.append(joined.at(it_pointer->second));
					joined_string.append(joined_rel);
					joined_string.push_back('|');
					it=rel_hash.find(joined_rel);
					tuples = GetTuples(joined_rel);
					relFactor=relFactor*tuples;
					//update the attribute vector for the new relation
					joined_attrs.insert(joined_attrs.end(),it->second.second.begin(),it->second.second.end());
					rel_hash.erase(it);
					token=strtok((char*)joined_rel.c_str(),"|");

					while(token!=NULL){
						it_set=myset.find(token);

						if(it_set!=myset.end()){

							it_pointer=pointer.find(token);
							it_pointer->second=joinCount;
							myset.erase(it_set);
						}
						else{
							cerr<<"Error : Improper Join in Apply"<<endl;
							exit(-1);
						}
						token=strtok(NULL,"|");
					}
				}
			}
			total=andFactor*relFactor;

			if(joined_string.size()>0)
				joined_string.erase(joined_string.size()-1,1);

			joined.push_back(joined_string);
			joinCount++;
			pair<int,vector<string> >  value;
			value.first=ceil(total-0.5);
			value.second=joined_attrs;
			rel_hash[joined_string]=value;
	}

long double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin){
	long double total=1.0;
	long double relFactor=1.0;
	long double tuples=0.0;
	set<string> myset;
	set<string>::iterator it_set;
	map<string,int>::iterator it_pointer;
	char* token;

	for(int i=0;i<numToJoin;i++){
		myset.insert(relNames[i]);
	}

	while(myset.size()>0){
		string front=*(myset.begin());

		it_pointer=pointer.find(front);
		if(it_pointer==pointer.end()){
			cerr<<"Relation in relNames does not exist in Statistics"<<endl;
			exit(-1);
		}
		else if(it_pointer->second==-1){ //relation exists singly
			tuples= GetTuples(front);
			relFactor=relFactor*tuples;
			myset.erase(myset.begin());

		}

		else{//relation has been joined


			string joined_rel;

			joined_rel.append(joined.at(it_pointer->second));

			tuples= GetTuples(joined_rel);
			relFactor=relFactor*tuples;

		token=strtok((char*)joined_rel.c_str(),"|");


			while(token!=NULL){
				it_set=myset.find(token);

				if(it_set!=myset.end()){
					myset.erase(it_set);
				}
				else{
					PrintAttHash();
					PrintJoined();
					PrintPointer();



					cerr<<"Error : Improper Join"<<endl;
					exit(-1);
				}
				token=strtok(NULL,"|");
			}

		}

	}


	// now we go through and build the comparison structure
		double dist_l=0.0;
		double dist_r=0.0;
		double andFactor=1.0;
		map<string,double> or_eval_map;
		map<string,double>::iterator it_or_eval_map;
		for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {

			// see if we have run off of the end of all of the ANDs
			if (parseTree == NULL) {
				break;
			}

			// we have not, so copy over all of the ORs hanging off of this AND
			struct OrList *myOr = parseTree->left;

			double orFactor=1.0;

			or_eval_map.clear();
			for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

				// see if we have run off of the end of the ORs
				if (myOr == NULL) {
					break;
				}

				// we have not run off the list, so add the current OR in!

				if(myOr->left->code == EQUALS){



					if(myOr->left->left->code == NAME && myOr->left->right->code == NAME){
						dist_l=GetDistinct(myOr->left->left->value);
						dist_r= GetDistinct(myOr->left->right->value);

						orFactor*=1-1/((dist_l>dist_r)?dist_l:dist_r);


					}

					else if (myOr->left->left->code == NAME) {
						dist_l=GetDistinct(myOr->left->left->value);
						//orFactor*=1-1/dist_l;
						it_or_eval_map=or_eval_map.find(myOr->left->left->value);
						if(it_or_eval_map==or_eval_map.end())
							or_eval_map[myOr->left->left->value]=1.0/dist_l;
						else
							or_eval_map[myOr->left->left->value]+=1.0/dist_l;
					}

					else if (myOr->left->right->code == NAME) {
						dist_r=GetDistinct(myOr->left->right->value);
						//orFactor*=1-1/dist_r;
						if(it_or_eval_map==or_eval_map.end())
								or_eval_map[myOr->left->left->value]=1.0/dist_r;
						else
								or_eval_map[myOr->left->left->value]+=1.0/dist_r;
					}
					else{
						cerr<<"Error : Not a valid CNF"<<endl;
						exit(1);
					}
				}

				//not equals
				else{
						if(myOr->left->right->code == NAME && att_hash.find(myOr->left->right->value)==att_hash.end() || myOr->left->left->code == NAME && att_hash.find(myOr->left->left->value)==att_hash.end()){
							cerr<<"Error : Not a valid CNF"<<endl;
							exit(1);
						}

							orFactor=orFactor*2.0/3.0;

				}


			}//or loop ends
			for(it_or_eval_map=or_eval_map.begin();it_or_eval_map!=or_eval_map.end();it_or_eval_map++){
				orFactor*=(1-(it_or_eval_map->second >1?1:it_or_eval_map->second));
			}



			andFactor*=(1-orFactor);

		}

		total=(long double)andFactor*relFactor;
		return total;
}

void Statistics::PrintAttHash(){

	cerr<<"Print Attribute Hash"<<endl<<endl;
	for(it_att_hash=att_hash.begin();it_att_hash!=att_hash.end();it_att_hash++)
			cerr<<it_att_hash->first<<":"<<it_att_hash->second.first<<":"<<it_att_hash->second.second<<endl;

}

int Statistics::GetDistinct(char* name){
	int value=0;

	it_att_hash=att_hash.find(name);

	if(it_att_hash!=att_hash.end()){
			value=it_att_hash->second.first;
			return value;
	}

	else{
		cerr<<"Error : Attribute not found in GetDistinct";
		exit(-1);
	}

}

int Statistics::GetTuples(string name){

	it=rel_hash.find(name);
	if(it!=rel_hash.end()){
		return it->second.first;
	}

	else{
		cerr<<"Error : Relation "<<name<<" not found in GetTuples"<<endl;
		exit(-1);
	}
}

void Statistics::UpdateAtt(char *attName, int numDistincts){

	it_att_hash=att_hash.find(attName);
	if(it_att_hash!=att_hash.end())
			it_att_hash->second.first=numDistincts;
	else{
		cerr<<"Error : Attribute not found for updation"<<endl;
		exit(-1);
	}
}

void Statistics::PrintJoined(){
	cerr<<endl<<"Print Joined Vector"<<endl;
	for(int i=0;i<joined.size();i++){
		cerr<<joined.at(i)<<endl;
	}

}

void Statistics::PrintPointer(){
	cerr<<endl<<"Print Pointer Map"<<endl;
	for(it_pointer=pointer.begin();it_pointer!=pointer.end();it_pointer++)
		cerr<<it_pointer->first<<" : "<<it_pointer->second<<endl;

}

void Statistics:: WritePointer(char* fromWhere){
	ofstream pointer_file;
	pointer_file.open(fromWhere);

	for(it_pointer=pointer.begin();it_pointer!=pointer.end();it_pointer++)
			pointer_file<<it_pointer->first<<":"<<it_pointer->second<<endl;
	pointer_file.close();
}

void Statistics:: WriteJoined(char* fromWhere){
	ofstream joined_file;
	joined_file.open(fromWhere);
	joined_file<<joinCount<<endl;
	for(int i=0;i<joined.size();i++){
			joined_file<<joined.at(i)<<endl;
		}
	joined_file.close();
}

void Statistics::ReadPointer(char* fromWhere){

	pointer.clear();
	ifstream myfile (fromWhere);
	string line;
	char *lstr;
	char *fstr;
	char *sstr;

	if (myfile.is_open()){
		while ( !myfile.eof() ){

			getline (myfile,line);
			if(line.size()<=0)
				break;

			lstr = new char [line.size()+1];
			strcpy (lstr, line.c_str());


			fstr=strtok (lstr,":");
			sstr=strtok(NULL," ");

			pointer[fstr]=atoi(sstr);
			delete[] lstr;
		}
	}
	myfile.close();
}


void Statistics::ReadJoined(char* fromWhere){

	joined.clear();
	ifstream myfile (fromWhere);
	string line;
	char *lstr;

	if (myfile.is_open()){

			if(!myfile.eof()){
				getline (myfile,line);
				if(line.size()!=0)
						joinCount=atoi((char*)line.c_str());
			}

			while ( !myfile.eof() ){

				getline (myfile,line);
				if(line.size()<=0)
					break;

				lstr = new char [line.size()+1];
				strcpy (lstr, line.c_str());
				joined.insert(joined.end(),lstr);
				delete[] lstr;
		}
	}
	myfile.close();
}
