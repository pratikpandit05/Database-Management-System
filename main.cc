#include <algorithm>
#include <iostream>
#include "Statistics.h"
#include "ParseTree.h"
#include <set>
#include <vector>
#include "Defs.h"
#include <string>
#include "limits.h"
#include "Pipe.h"
#include "Command.h"
#include <ctime>
#include "Schema.h"
#include "Query.h"
#include <sys/time.h>
#define p(EX) cerr << #EX << ": " << (EX) << endl;

using namespace std;

char *catalog_path = "catalog"; // full path of the catalog file

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" {
	int yyfuncparse(void);   // defined in y.tab.c
}

extern "C" char* outFile;
extern "C" int create;
extern "C" int query;
extern "C" int insert;
extern "C" int drop;
extern "C" int output;






char *q[]={
		   	   "SELECT SUM (ps.ps_supplycost) FROM part AS p, supplier AS s, partsupp AS ps WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0)",
		       "SELECT SUM (c.c_acctbal) FROM customer AS c, orders AS o WHERE (c.c_custkey = o.o_custkey) AND (o.o_totalprice < 10000.0)",
		       "SELECT l.l_orderkey, l.l_partkey, l.l_suppkey FROM lineitem AS l WHERE (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL') AND (l.l_orderkey > 5000) AND (l.l_orderkey < 6000)",
		       "SELECT ps.ps_partkey, ps.ps_suppkey, ps.ps_availqty FROM partsupp AS ps WHERE (ps.ps_partkey < 100) AND (ps.ps_suppkey < 50)",
		       "SELECT SUM (l.l_discount) FROM customer AS c, orders AS o, lineitem AS l WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND (l.l_discount < 0.03)",
		       "SELECT DISTINCT s.s_name FROM supplier AS s, part AS p, partsupp AS ps WHERE (s.s_suppkey = ps.ps_suppkey) AND (p.p_partkey = ps.ps_partkey) AND (p.p_mfgr = 'Manufacturer#4') AND (ps.ps_supplycost < 350.0)",
		       "SELECT SUM (l.l_extendedprice * (1 - l.l_discount)), l.l_orderkey, o.o_orderdate, o.o_shippriority FROM customer AS c, orders AS o, lineitem AS l  WHERE (c.c_mktsegment = 'BUILDING') AND  (c.c_custkey = o.o_custkey) AND (l.l_orderkey = o.o_orderkey) AND (l.l_orderkey < 100 OR o.o_orderkey < 100) GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority",
		 	    "SELECT SUM (ps.ps_supplycost), s.s_suppkey FROM part AS p, supplier AS s, partsupp AS ps WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0) GROUP BY s.s_suppkey",
		        "SELECT SUM (c.c_acctbal),c.c_name FROM customer AS c, orders AS o WHERE (c.c_custkey = o.o_custkey) AND (o.o_totalprice < 10000.0) GROUP BY c.c_name",
		        "SELECT l.l_orderkey, l.l_partkey, l.l_suppkey FROM lineitem AS l WHERE (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL')",
		        "SELECT DISTINCT c1.c_name, c1.c_address, c1.c_acctbal FROM customer AS c1, customer AS c2 WHERE (c1.c_nationkey = c2.c_nationkey) AND      (c1.c_name ='Customer#000070919')",
		        "SELECT SUM(l.l_discount) FROM customer AS c, orders AS o, lineitem AS l WHERE (c.c_custkey = o.o_custkey) AND       (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND (l.l_discount < 0.03)",
		        "SELECT l.l_orderkey FROM lineitem AS l WHERE (l.l_quantity > 30.0)",
		        "SELECT DISTINCT c.c_name FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r WHERE (l.l_orderkey = o.o_orderkey) AND (o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)",
		        "SELECT l.l_discount FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r WHERE (l.l_orderkey = o.o_orderkey) AND (o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey) AND (r.r_regionkey = 1) AND (o.o_orderkey < 10000)",
		        "SELECT SUM (l.l_discount) FROM customer AS c, orders AS o, lineitem AS l WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND    (l.l_discount < 0.03)",
		        "SELECT SUM (l.l_extendedprice * l.l_discount) FROM lineitem AS l WHERE (l.l_discount<0.07) AND (l.l_quantity < 24.0)"
};




int main () {



int option;

	while (true){
		cerr<<"***********************************************************************************\n";
	/*		for(int i=0;i<17;i++){
				cerr<<(i+1)<<". "<<q[i]<<endl;
			}*/
			cout << "\n select option: \n";
			cout << "\t 1. Pre-loaded Query Plan\n";
			cout << "\t 2. Pre-loaded Queries\n";
			cout << "\t 3. Execute Query \n";
			cout << "\t 0. Exit \n";
			cin >> option;


	if(option==0){
		cout<<"Exiting...";
		exit(-1);
	}
	else if(option==1){
			int qno=0;

			do{
			cout<<"Enter query index [1-17] . 0 to Exit\t";
			cin>>qno;

				if(qno >=1 && qno<=17){
					 yy_scan_string(q[qno-1]);
					int res = yyparse();
					if(res!=0)
						continue;

					Query q;
					q.showPlan();
				}
				else if(qno ==0){
					cerr<<"Exiting...\n";
					exit(-1);
				}
				cerr<<"***********************************************************************************\n";
			}while(true);
	}
	else if(option ==2){
		int qno=0;

				do{
				cout<<"Enter query index [1-17] . 0 to Exit\t";
				cin>>qno;

					if(qno >=1 && qno<=17){
						 yy_scan_string(q[qno-1]);
						int res = yyparse();

						if(res!=0)
							continue;
						std::time_t start = std::clock();
						Query q;
						q.run();
						std::time_t stop = std::clock();
						cerr << "Time taken: " << static_cast<double> (stop - start)/ CLOCKS_PER_SEC << " secs\n";

					}
					else if(qno ==0){
						cerr<<"Exiting...\n";
						exit(-1);
					}
					else{
						cout<<"\n Please enter a valid option \n";
					}
					cerr<<"***********************************************************************************\n";
				}while(true);


	}
	else if(option==3){

		do{
		cout<<"Enter Query \n";

			int res=yyparse();
			if(res!=0){
				cerr<<"Exiting...\n";
				exit(-1);
			}

			 if(create){
					Command ddl;
					 ddl.createTable();
				 	 create=0;
			 }
				 else if(insert){
						Command ddl;
					 	 ddl.insertIntoTable();
					 	 insert=0;
				 }
				 else if(drop){
					 Command ddl;
					 ddl.dropTable();
					 drop=0;
				 }
				 else if(query){



						if(output==0){
							Query q;
							q.showPlan();
						}
						else if(output==1){
							timespec time1, time2;
							double totalTime;
							clock_gettime(CLOCK_REALTIME, &time1);
							Query q;
							q.run();
							clock_gettime(CLOCK_REALTIME, &time2);
							totalTime = time2.tv_sec - time1.tv_sec;
							cout << "TotalTime Taken: " << totalTime << " secs" << endl;
						}
						else if(output==2){
							timespec time1, time2;
							double totalTime;
							clock_gettime(CLOCK_REALTIME, &time1);
							Query q;
							q.writeFile=true;
							q.run();
							clock_gettime(CLOCK_REALTIME, &time2);
							totalTime = time2.tv_sec - time1.tv_sec;
							cout << "TotalTime Taken: " << totalTime << " secs" << endl;
						}
						query=0;
				 }
				 else{
					 cerr<<"Setting Output Mode to :"<<output<<endl;
					 if(output ==2)
						 cerr<<"Output File "<<outFile<<endl;
				 }
		}while(true);

		}
			else
					cout<<"\n Please enter a valid option \n";
	}//end of while



}


