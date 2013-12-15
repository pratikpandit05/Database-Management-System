#include "GenericDBFile.h"
#include "DBFile.h"



SortedDBFile::SortedDBFile(SortInfo *info){

	sortInfo = info;
	buffsz = 100;
	queryBuilt=false;
	sortOrderExists=true;
	bigq=NULL;
}

int SortedDBFile::Create(char * f_path){
	mode=Reading;
	return GenericDBFile::Create(f_path);
}

int SortedDBFile::Open(char * f_path){
	mode=Reading;
	return GenericDBFile::Open(f_path);
}

void SortedDBFile::InitBigQ(){
		input = new Pipe(buffsz);
		output = new Pipe(buffsz);
		bigq=new BigQ(*input, *output,*(sortInfo->myOrder),sortInfo->runLength);
}


void SortedDBFile::Load(Schema &myschema, char *loadpath){

		FILE *tbl_path = fopen (loadpath, "r");												//open the tbl file
		curPageIndex=0;																		//set the current page index for the bin file to 0
		Record temp;
		int counter=0;
		curPage.EmptyItOut();																//Empty the page before starting to load
		while (temp.SuckNextRecord (&myschema, tbl_path) == 1) {
			counter++;
			if (counter % 10000 == 0)
				cerr <<".";
				Add(temp);																		// add record to page
		}
		fclose(tbl_path);																	//close the tbl file
}


 void SortedDBFile::Add (Record &addme){
	 	 if(mode==Reading){
	 		 	if(!bigq)
	 		 		 	 InitBigQ();
	 					mode=Writing;
	 	 }

	 	input->Insert(&addme);
 }
int SortedDBFile ::GetNextQuery(Record &fetchme, CNF &cnf, Record &literal){

	 	 ComparisonEngine ceng;
 	 	 	 while(1){
										if(curPage.GetFirst(&fetchme)==1 ){												//if record exists in the page

												if(ceng.Compare (&literal, query, &fetchme,sortInfo->myOrder) ==0){//if the query sort order is matching with the record

													if (ceng.Compare (&fetchme, &literal, &cnf))								//if the CNF is matching with the record
															return 1;
											}

											else
												return 0;
										}


										else{																			//records in the page are over

												curPageIndex++;																//increase the page index
												if(curPageIndex<bin_file.GetLength()-1)										//curPageIndex is less than the number of pages in the file(  curPageIndex starts with 0, so when we give 0 , we get the 1st page. thus curpageIndex less than curlength-1)
														bin_file.GetPage(&curPage,curPageIndex);								//load the next page from the file
												else																		// No page exists
														return 0;
										}
									}
 }

 int SortedDBFile::SequentialGetNext (Record &fetchme, CNF &cnf, Record &literal) {

 	ComparisonEngine comp;

 	while(1)
 	{
 		if(curPage.GetFirst(&fetchme)==1 )												//if record exists in the page
 		{
 			if (comp.Compare (&fetchme, &literal, &cnf))								//if the CNF is matching with the record
 				return 1;
 		}
 		else																			//records in the page are over
 		{
 			curPageIndex++;																//increase the page index
 			if(curPageIndex<bin_file.GetLength()-1)										//curPageIndex is less than the number of pages in the file(  curPageIndex starts with 0, so when we give 0 , we get the 1st page. thus curpageIndex less than curlength-1)
 				bin_file.GetPage(&curPage,curPageIndex);								//load the next page from the file
 			else																		// No page exists
 				return 0;
 		}
 	}

 }

 int SortedDBFile::GetNext (Record &fetchme){
	 if(mode==Writing){
	 		 Merge();
	 }
 	return GenericDBFile::GetNext(fetchme);
 }



 int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal){


 	 if(mode==Writing){

 		 Merge();
 		 queryBuilt=false;
 		 sortOrderExists=true;
 	 }

 	 if(sortOrderExists){
			 if(!queryBuilt){
				  queryBuilt=true;
				 int search;


				 	 query=new OrderMaker;
			 			 if(cnf.GetQuerySortOrders(*query,*(sortInfo->myOrder))>0){  //query sort order exists

			 				 //query->Print();
			 				 search=BinarySearch(fetchme,cnf,literal);

								  ComparisonEngine ceng;

								  if(search){
										 if (ceng.Compare (&fetchme, &literal, &cnf))								//if the CNF is matching with the record
													return 1;


										 else{
											GetNextQuery(fetchme, cnf, literal);
												 }
										  }

									  else{
										  return 0;
									  }
								 }
							 else{//Handle sequential search
								 sortOrderExists=false;
								 return SequentialGetNext(fetchme, cnf, literal);
							 }
					 }

					 else{
						GetNextQuery(fetchme, cnf, literal);
					 }
			 }
 	 else{
 		return SequentialGetNext(fetchme, cnf, literal);
 	 }

 }

 int SortedDBFile::BinarySearch(Record& fetchme,CNF &cnf,Record &literal){

	 cerr<<"Binary Search"<<endl;
	 off_t first=curPageIndex;
	 off_t last=bin_file.GetLength()-2;

	 Record *currentRecord = new Record;
	 Page *midPage=new Page;
	 bool found=false;  //flag for whether a matching attribute to query was found
	 ComparisonEngine ceng;
	 off_t mid=first;

	 while(first < last){
					  mid=(first+last)/2;

					 bin_file.GetPage(midPage,mid);

								 if(midPage->GetFirst(&fetchme)==1 ){												//if record exists in the page
									 if (ceng.Compare (&literal, query, &fetchme,sortInfo->myOrder) <= 0){  //fetchme > literal
										 last = mid;
										}
										else{
											first=mid;

											if(first == last -1){
												bin_file.GetPage(midPage,last);
												 midPage->GetFirst(&fetchme);

											 if (ceng.Compare (&literal, query, &fetchme,sortInfo->myOrder) > 0)
																 	 mid=last;
												break;
											}
										}
								}
								 else
									 break;
	 }



	 //Scanning from page mid till end page

	 if(mid==curPageIndex){		//if mid points to curPage then we need to scan from current record till end of page
		 	 while(curPage.GetFirst(&fetchme)==1){
		 		if( ceng.Compare (&literal, query, &fetchme,sortInfo->myOrder) == 0 ){
		 									found=true;
		 									break;
		 							}
		 	 }

	 }
	 else{		//if mid is some other than current page then we need to load the page and scan it till end
		 bin_file.GetPage(&curPage,mid);


			 while(curPage.GetFirst(&fetchme)==1){
						if( ceng.Compare (&literal, query, &fetchme,sortInfo->myOrder) == 0 ){
								found=true;
								curPageIndex=mid;
								break;
						}
			 }


	 }

	 //if no record was found then record might exist in the first location of next page
	if(!found && mid < bin_file.GetLength()-2){
					 bin_file.GetPage(&curPage,mid+1);
					 if(curPage.GetFirst(&fetchme)==1 && ceng.Compare (&literal, query, &fetchme,sortInfo->myOrder) == 0){
						 found=true;
						 curPageIndex=mid+1;
					 }
	 }


	 if(!found)
		 return 0;
	 else
		 return 1;

 }

 int SortedDBFile::Merge(){

 	 int err = 0;
 	 int i = 0;

 	 input->ShutDown();

 	mode=Reading;
 	if(bin_file.GetLength()!=0){
 		 MoveFirst();
 	 }

 	Record *last = NULL, *prev = NULL;
 	Record *fromFile=new Record;
 	Record *fromPipe=new Record;
 	DBFile dbfile;
 	dbfile.Create("heap.tmp",heap,NULL);
 	int filecount=0,pipecount=0,totalcount=0;

 	int fp=output->Remove (fromPipe);
 	int ff=GetNext(*fromFile);

 	ComparisonEngine ceng;

 	while (1) {

 		if (fp && ff) {
 			if (ceng.Compare (fromPipe, fromFile, sortInfo->myOrder) >= 1) {  // returns a -1, 0, or 1 depending upon whether left is less then, equal to, or greater
 															// than right, depending upon the OrderMaker.So a 1 means unsorted pair has been encountered
 				dbfile.Add(*fromFile);
 				++filecount;
 				ff=GetNext(*fromFile);

 			}
 			else{
 				dbfile.Add(*fromPipe);
 				++pipecount;
 				fp=output->Remove (fromPipe);

 			}
 	 	}
 		else{
 			break;
 		}

 	}

 	while(ff){
 		dbfile.Add(*fromFile);
 		++filecount;
 		ff=GetNext(*fromFile);
 	}

 	while(fp){
 	 		dbfile.Add(*fromPipe);
 	 		++pipecount;
 	 		fp=output->Remove (fromPipe);

 	 	}

 	//cout<<"File Count	"<<filecount;
 	//cout<<"Pipe Count	"<<pipecount;
 	delete fromFile;
 	delete fromPipe;
 	delete input;
 	delete output;
 	delete bigq;

 	dbfile.Close();

 	//cout<<"Path is"<<fpath<<endl;
 	remove (fpath);
 	remove("heap.tmp.md");
 	rename("heap.tmp",fpath);



 }


 void SortedDBFile::MoveFirst () {

	if(mode==Writing)
	{
		Merge();

	}
 	curPageIndex=0;																		//set the page index to the first page
 	curPage.EmptyItOut();																// empty the curpage first
 	if(bin_file.GetLength()>0)
 			bin_file.GetPage(&curPage,curPageIndex);											// Load the first page from the bin file to the current Page
 	queryBuilt=false;
 	sortOrderExists=true;
 }


 int SortedDBFile::Close(){

	 if(mode==Writing){
	 	 Merge();

 	 }
	 bin_file.Close();
	 if(input){

	 }
 }

 SortedDBFile::~SortedDBFile(){
	 delete sortInfo->myOrder;
	 delete sortInfo;
	 delete query;
}
