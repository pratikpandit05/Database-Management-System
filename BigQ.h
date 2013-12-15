#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <queue>
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <algorithm>
#include <ctime>
#include "Pipe.h"
#include "File.h"
#include "Record.h"


using namespace std;


	typedef struct {
		Pipe *inputPipe;
		Pipe *outputPipe;
		OrderMaker *order;
		int runlen;
	}BigQutil;
	

typedef priority_queue<RunRecord* , vector<RunRecord*> ,RunRecordComparison> RunRecordPQ ;


class BigQ {
	
public:
	

	friend void *doTpmms ( void* arg);
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	
};

#endif
