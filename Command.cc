#include "Command.h"

extern "C" int created;
extern "C" struct Table *tb;
extern "C" struct AttList attlist;
extern "C" char *myfile;
extern "C" char *droptable;
extern "C" char *mytable;
extern "C" struct NameList *sortAtts;
extern "C" char* catalog_path;
extern "C" void printAndList(AndList* boolean);


		void Command:: createTable(){
		    cerr<<tb->name<<" : "<< tb->type<<endl;
			char path[100];
			sprintf(path,"%s.bin",tb->name);
			int attCnt=-1;
			AttList  *att=tb->att;
			while(att){
				att=att->next;
				attCnt++;
			}
			att=tb->att;
			Schema *sch=new Schema(att, attCnt);
			sch->WriteSchema(tb->name);
			DBFile dbfile;
			cerr<<tb->type<<endl;
			if(tb->type==1){
				cerr<<"Heap DB"<<endl;
				dbfile.Create(path,heap,NULL);
				dbfile.Close();
			}
			else if(tb->type==2){
				int count=0,size;
				NameList *temp=sortAtts;
				while(temp){
					temp=temp->next;
					count++;
				}
				size=count;
				char *atts[count];
				temp=sortAtts;
				while(temp){
					atts[--count]=temp->name;
					temp=temp->next;
				}

				AndList* sortList;
				AndList *tem=NULL;
				for(int i=0;i<size;i++){
					cerr<<atts[i];
					if(tem==NULL){
							tem=new AndList;
							sortList=tem;
							}
							else{
								tem->rightAnd=new AndList;
								tem=tem->rightAnd;
							}

					Operand *left=new Operand;
					left->code=NAME;
					left->value=strdup(atts[i]);

					Operand *right=new Operand;
					right->code=NAME;
					right->value=strdup(atts[i]);



					ComparisonOp *comOp=new ComparisonOp;
					comOp->code=EQUALS;
					comOp->left=left;
					comOp->right=right;

					OrList* orlist=new OrList;
					orlist->left=comOp;
					orlist->rightOr=NULL;

					tem->left=orlist;
					tem->rightAnd=NULL;
				}


				CNF *cnf = new CNF;
				Record *literal = new Record;

				cnf->GrowFromParseTree(sortList,sch,*literal);


				OrderMaker *sortOrder = new OrderMaker;
				OrderMaker dummy;


				cnf->GetSortOrders(*sortOrder,dummy);
				SortInfo *sortInfo=new SortInfo;
				sortInfo->myOrder=sortOrder;
				sortInfo->runLength=10;
				//struct {OrderMaker *o; int l;} startup = {sortOrder, 10};
				dbfile.Create (path, sorted, sortInfo);




			}
			dbfile.Close();
		}


		void Command:: insertIntoTable(){

			DBFile dbfile;
			char path[100];
			sprintf(path,"%s.bin",mytable);
			dbfile.Open(path);
			//Schema *sch=new Schema(mytable);
			Schema *sch=new Schema(catalog_path,mytable);
			dbfile.Load(*sch,myfile);
			dbfile.Close();

		}

		void Command:: dropTable(){

			char path[100];
			sprintf(path,"%s.bin",mytable);
			char schpath[100];
			sprintf(schpath,"%s.sch",mytable);
			char metapath[100];
			sprintf(metapath,"%s.bin.md",mytable);
			remove(path);
			remove(schpath);
			remove(metapath);

		}
