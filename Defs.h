#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};
enum Mode{Reading,Writing};
typedef enum {heap, sorted, tree} fType;
enum OpType {selectF,selectP,join,groupBy,project,distinct,sum,writeOut};

unsigned int Random_Generate();




#endif
