#ifndef COMMAND_H_
#define COMMAND_H_

#include "DBFile.h"
#include <stdio.h>
#include <iostream>
#include "ParseTree.h"
#include "Defs.h"
#include "Schema.h"
#include "Query.h"

class Command{
public:
		void createTable();
		void insertIntoTable();
		void dropTable();

};

#endif /* COMMAND_H_ */
