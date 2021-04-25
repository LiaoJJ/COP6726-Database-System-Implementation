CC = g++ -std=c++11 -Wno-deprecated

tag = -i
gtest_tag = -std=c++11 -lgtest -lgtest_main


ifdef linux
tag = -n
endif

a5.out:   y.tab.o lex.yy.o RelOp.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o DBFileHeap.o DBFileSorted.o DBFileTree.o NodeForQuery.o Statistics.o main.o
	$(CC) -o a5.out y.tab.o lex.yy.o RelOp.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o DBFileHeap.o DBFileSorted.o DBFileTree.o NodeForQuery.o Statistics.o main.o -lpthread
	
gtest: y.tab.o lex.yy.o RelOp.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o DBFileHeap.o DBFileSorted.o DBFileTree.o NodeForQuery.o Statistics.o gtest.o
	$(CC) -o gtest.out y.tab.o RelOp.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o DBFileHeap.o DBFileSorted.o DBFileTree.o NodeForQuery.o Statistics.o gtest.o $(gtest_tag)

main.o : main.cc
	$(CC) -g -c main.cc

gtest.o: gtest.cpp
	$(CC) -g -c gtest.cpp $(gtest_tag)

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

DBFileHeap.o: DBFileHeap.cc
	$(CC) -g -c DBFileHeap.cc

DBFileSorted.o: DBFileSorted.cc
	$(CC) -g -c DBFileSorted.cc

DBFileTree.o: DBFileTree.cc
	$(CC) -g -c DBFileTree.cc

NodeForQuery.o: NodeForQuery.cc
	$(CC) -g -c NodeForQuery.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
