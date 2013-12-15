#include "GenericDBFile.h"
HeapDBFile::HeapDBFile(){

}


int HeapDBFile::Create(char * f_path){

	toWrite=true;
	return GenericDBFile::Create(f_path);
}

int HeapDBFile::Open(char * f_path){

	toWrite=false;
	return GenericDBFile::Open(f_path);
}


void HeapDBFile::Load (Schema &f_schema, char *loadpath) {



	FILE *tbl_path = fopen (loadpath, "r");												//open the tbl file
	curPageIndex=0;																		//set the current page index for the bin file to 0
	Record temp;
	int counter=0;
	curPage.EmptyItOut();																//Empty the page before starting to load
	while (temp.SuckNextRecord (&f_schema, tbl_path) == 1) {
		counter++;

		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		Add(temp);																		// add record to page

	}

	bin_file.AddPage(&curPage,curPageIndex++);											//dirty write
	curPage.EmptyItOut();																//empty the last page
	fclose(tbl_path);																	//close the tbl file

}






void HeapDBFile::Add (Record &rec) {


	if(curPage.Append(&rec)==0){														// append record to the page.if page is full return 0
			bin_file.AddPage(&curPage,curPageIndex++);									// since page is full , add page to file

			curPage.EmptyItOut();														// empty the page
			curPage.Append(&rec);														// append the record to the new page
		}

}

int HeapDBFile::GetNext (Record &fetchme){

	return GenericDBFile::GetNext(fetchme);

}


int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

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

void HeapDBFile::MoveFirst () {

	curPageIndex=0;																		//set the page index to the first page
	curPage.EmptyItOut();																// empty the curpage first
	bin_file.GetPage(&curPage,curPageIndex);											// Load the first page from the bin file to the current Page

}




int HeapDBFile::Close(){


	if(toWrite && curPage.getCurSizeInBytes()>0){

			bin_file.AddPage(&curPage,curPageIndex++);											//dirty write
			curPage.EmptyItOut();																					//empty the last page
			toWrite=false;
	}

		bin_file.Close();																							//close the bin file
		return 1;

}

HeapDBFile::~HeapDBFile(){
}


