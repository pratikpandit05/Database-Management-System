#include "BigQ.h"



void *doTpmms(void* arg) {

	static int tmpCount;
	//cerr<<"Inside doTpmms"<<endl;
	std::time_t start = std::clock();

	char tempFile[100];
	sprintf(tempFile,"buffer%d.tmp",tmpCount++);
	cout<<tempFile<<endl;

	//Internal structure used to pass info from BigQ constructor to thread function
	BigQutil *bq = (BigQutil *) arg;
	RecordComparison recCompare(bq->order);

	//Vector of records
	vector<Record*> recVector;

	//Priority Queue to sort records from runs
	RunRecordPQ runRecordPQ(RunRecordComparison(bq->order));

	File tmp_file;
	Page curPage;

	//Init curPageIndex
	off_t curPageIndex = 0;
	curPage.EmptyItOut();
	Record temp, *rec;
	int counter = 0, popped = 0, pushed = 0;

	//Open a temporary file for writing sorted runs
	tmp_file.Open(0, tempFile);

	//Total size of each run
	int max_size = bq->runlen * PAGE_SIZE;

	//Init curSizeInBytes
	int curSizeInBytes = sizeof(int);
	int recSize = 0;

	//Vector to keep the start index of page for the runs
	vector<int> pageOffsets;
	int runCount = 0;

	//Repetedly remove records from input pipe till it is exhausted
	while (bq->inputPipe->Remove(&temp) == 1) {


		recSize = (&temp)->GetSize(); //((int *) b)[0];

		Record *newrec = new Record;
		newrec->Consume(&temp);

		//If total size has not exceeded max size of run,
		//Increase current size of current run and
		//push record into Priority Queue
		if (curSizeInBytes + recSize <= max_size) {
			recVector.push_back(newrec);
			pushed++;
			curSizeInBytes += recSize;
		}

		//If run has reached max size
		else {
			runCount++;
			pageOffsets.push_back(curPageIndex);
			//Pop all records from PQ and insert into file
			sort(recVector.begin(), recVector.end(), recCompare);

			for (int i = 0; i < recVector.size(); i++) {
				rec = recVector.at(i);
				// append record to the page.if page is full return 0
				if (curPage.Append(rec) == 0) {

					// if page is full , add page to full
					tmp_file.AddPage(&curPage, curPageIndex++);

					curPage.EmptyItOut();

					// append the record to the new page
					curPage.Append(rec);

				}
				delete rec;


			}

			recVector.clear();

			//this is absolutely desirable as these are sorted records whihc have not been written
			//at the end of the run
			if (curPage.getCurSizeInBytes() > 0) {
				tmp_file.AddPage(&curPage, curPageIndex++);
				curPage.EmptyItOut();
			}

			recVector.push_back(newrec);
			curSizeInBytes = sizeof(int) + recSize;

		}

	}

	//sort the records of the last run when the run has not reached max size
	sort(recVector.begin(), recVector.end(), recCompare);

	// pop the records of the last run when the run has not reached max size
	pageOffsets.push_back(curPageIndex);

	for (int i = 0; i < recVector.size(); i++) {
		rec = recVector.at(i);
		// append record to the page.if page is full return 0
		if (curPage.Append(rec) == 0) {
			tmp_file.AddPage(&curPage, curPageIndex++); // if page is full , add page to full
			curPage.EmptyItOut(); // empty the page
			curPage.Append(rec); // append the record to the new page
		}
		delete rec;
	}
	recVector.clear();

	if (curPage.getCurSizeInBytes() > 0) {
		tmp_file.AddPage(&curPage, curPageIndex++);
		curPage.EmptyItOut();
	}
	pageOffsets.push_back(curPageIndex);


	cout<<"Merging runs . . ."<<endl;
	int numOfRuns = pageOffsets.size() - 1;
	Run *runs[numOfRuns];
	for (int i = 0; i < numOfRuns; i++) {
		Record* tmprec = new Record;

		runs[i] = new Run(&tmp_file, pageOffsets[i], pageOffsets[i + 1]);
		runs[i]->GetNext(tmprec);

		RunRecord* runRecord = new RunRecord(tmprec, runs[i]);
		runRecordPQ.push(runRecord);

	}
	RunRecord *poppedRunRecord;
	Record* poppedRecord;
	Run* poppedRun;

	while (!runRecordPQ.empty()) {
		poppedRunRecord = runRecordPQ.top();
		runRecordPQ.pop();
		poppedRun = poppedRunRecord->run;
		poppedRecord = poppedRunRecord->record;
		bq->outputPipe->Insert(poppedRecord);
		delete poppedRecord;
		Record* nextRecord = new Record;
		if (poppedRun->GetNext(nextRecord) == 1) {
			poppedRunRecord->record = nextRecord;
			runRecordPQ.push(poppedRunRecord);
		} else {
			delete poppedRun;
			delete poppedRunRecord;
			delete nextRecord;
		}

	}
	cout<<"Done Merging"<<endl;
	bq->outputPipe->ShutDown();
	tmp_file.Close();
	//cerr << "Time elapsed: " << static_cast<double> (clock() - start)/ CLOCKS_PER_SEC << " secs\n";
	remove(tempFile);
	//remove("buffer.tmp");
}

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	// read data from in pipe sort them into runlen pages

	if (runlen <= 0) {
		cout << "Invalid Runlength" << endl;
		exit(1);
	}

	pthread_t worker;
	BigQutil *util = new BigQutil{&in, &out, &sortorder, runlen};

	pthread_create(&worker, NULL, doTpmms, util);


	// construct priority queue over sorted runs and dump sorted data
	// into the out pipe




}

BigQ::~BigQ() {
}
