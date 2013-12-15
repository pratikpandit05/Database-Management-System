/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison interface for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2011 Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     Name = 258,
     Float = 259,
     Int = 260,
     String = 261,
     SELECT = 262,
     GROUP = 263,
     DISTINCT = 264,
     BY = 265,
     FROM = 266,
     DROP = 267,
     WHERE = 268,
     SUM = 269,
     AS = 270,
     AND = 271,
     OR = 272,
     CREATE = 273,
     TABLE = 274,
     INSERT = 275,
     INTO = 276,
     HEAP = 277,
     SORTED = 278,
     INTEGER = 279,
     DOUBLE = 280,
     STRING = 281,
     ON = 282,
     SET = 283,
     OUTPUT = 284,
     STDOUT = 285,
     NONE = 286
   };
#endif
/* Tokens.  */
#define Name 258
#define Float 259
#define Int 260
#define String 261
#define SELECT 262
#define GROUP 263
#define DISTINCT 264
#define BY 265
#define FROM 266
#define DROP 267
#define WHERE 268
#define SUM 269
#define AS 270
#define AND 271
#define OR 272
#define CREATE 273
#define TABLE 274
#define INSERT 275
#define INTO 276
#define HEAP 277
#define SORTED 278
#define INTEGER 279
#define DOUBLE 280
#define STRING 281
#define ON 282
#define SET 283
#define OUTPUT 284
#define STDOUT 285
#define NONE 286




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 2068 of yacc.c  */
#line 38 "Parser.y"

 	struct FuncOperand *myOperand;
	struct FuncOperator *myOperator; 
	struct TableList *myTables;
	struct ComparisonOp *myComparison;
	struct Operand *myBoolOperand;
	struct OrList *myOrList;
	struct AndList *myAndList;
	struct NameList *myNames;
	struct AttList *myAtt;
	char *actualChars;
	char whichOne;



/* Line 2068 of yacc.c  */
#line 128 "y.tab.h"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE yylval;


