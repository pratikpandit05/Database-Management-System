#ifndef GENERIC_DBFILE_H
#define GENERIC_DBFILE_H
#include "Record.h"
#include "Defs.h"
#include "File.h"
#include "Pipe.h"
#include <iostream>
#include "BigQ.h"
#include "Comparison.h"


#include <cstring>

class GenericDBFile {


protected:
	File bin_file;
	Page curPage;
	off_t curPageIndex;
	bool toWrite;
	char *fpath;
	bool merge;


public:



	virtual void MoveFirst ()=0;
	virtual int GetNext (Record &fetchme);
	virtual int Create(char *f_path);
	virtual int Open (char *f_path);
	virtual void Load (Schema &myschema, char *loadpath)=0;
	virtual void Add (Record &addme)=0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;
	virtual int Close()=0;
	virtual ~GenericDBFile();
};

class HeapDBFile : public GenericDBFile {

public:
		HeapDBFile();
		int Create(char *f_path);
		int Open (char *f_path);
		int GetNext (Record &fetchme);
		void Load (Schema &myschema, char *loadpath);
		void Add (Record &addme);
		void MoveFirst ();
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		int Close();
		~HeapDBFile();

};

class SortedDBFile : public GenericDBFile {
		BigQ *bigq;
		Mode mode;
		SortInfo *sortInfo;
		int buffsz; // pipe cache size
		Pipe *input;
		Pipe *output;
		bool queryBuilt;
		bool sortOrderExists;
		OrderMaker *query;
		Page midPage;


public:

		SortedDBFile();
		SortedDBFile(SortInfo* info);
		int Create(char *f_path);
		int Open (char *f_path);
		void InitBigQ();
		void MoveFirst ();
		virtual int GetNext (Record &fetchme);
		void Load (Schema &myschema, char *loadpath);
		void Add (Record &addme);
		int GetNextQuery(Record &fetchme, CNF &cnf, Record &literal);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		int SequentialGetNext (Record &fetchme, CNF &cnf, Record &literal);
		int BuildQuery(OrderMaker *cnfOrder, OrderMaker *sortOrder);
		int BinarySearch(Record& fetchme,CNF &cnf,Record &literal);
		int Merge();
		int Close();
		~SortedDBFile();
};
#endif
