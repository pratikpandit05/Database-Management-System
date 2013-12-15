#include "GenericDBFile.h"
#include "Defs.h"



int GenericDBFile::Create(char *f_path){

	curPageIndex=0;
	curPage.EmptyItOut();
	fpath=f_path;
	bin_file.Open(0,f_path);															//create a new bin file
	return 1;
}

int GenericDBFile::Open (char *f_path) {
	curPageIndex=0;
	curPage.EmptyItOut();
	fpath=f_path;
	bin_file.Open(1,f_path);
	return 1;
}



int GenericDBFile::GetNext (Record &fetchme) {

	if(curPage.GetFirst(&fetchme)==1)													//record exists in the page
		return 1;

	else{																				//records in the page are over

		curPageIndex++;
		if(curPageIndex<bin_file.GetLength()-1)											//next page exists
		{
			bin_file.GetPage(&curPage,curPageIndex);									//load the next page from file
			curPage.GetFirst(&fetchme);													//get the first record from the new page
			return 1;
		}
		else																			// no page exists
			return 0;
	}

}

GenericDBFile::~GenericDBFile(){
}


