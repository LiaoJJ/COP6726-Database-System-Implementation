# [proj2 Sorted File Implementation](/a2-2test)

- Haocheng Song(UFID: 11851321)
- Jiajing Liao(UFID: 01469951) 


## How to Run
For test.cc
```
make clean
make 
./runTestCases.sh
```

For gtest
```
make clean
make
./gtest.out
```


## major works explanation:

### create new Architecture base on interface DBFileGeneric

DBFile is an unified class, it will determine the file type on Create() or Open(), there is also a .meta file as metadata
DBFile will be a entrance, it will help to find the real implementation class
DBFileGeneric is an interface, it only includes pure virtual functions
DBFileHeap, DBFileSorted, DBFileTree is the real implementation of DBFile, they contains real logics. 

### lead into new metadata, including filetype, runLength and orderMaker

int DBFile::Create (char *f_path, fType f_type, void *startup): convert orderMaker into metadata file
int DBFile::Open (char *f_path): convert a metadata file into orderMaker

Definition to metadata:
first row: file type
second row: runLen
rest of rows: orderMaker

Example metadata file:
sorted	# file type
16 		# runLen
1 		# orderMaker.numAtts
2		# orderMaker.whichAtts[0]
Int		# orderMaker.whichTypes[0]

we use a fstream to write out or parse in metadata

### implement Sorted File 

void DBFileSorted::writeMode(): inin the BigQ Thread
void DBFileSorted::readMode(): merge 2 sorted files into the new sorted file

As you can see in below Logic picture.

When a new record come in, we add it into the BigQ, and then BigQ will insert into into Runs and then get a sorted dif file. When it is necessary, we will merge the dif file with the original sorted file with a 2-Way merge algorithm and get the new sorted file.

However, we will not merge the dif file with sorted file instantly. Since merging is a time consuming processes, we will only do mergeing when there is a read-write change, or file close() operations.

#### Read-Write Switch detail
From read to write, it will set up the BigQ class in a new Thread for processing the incoming added Records
From write to read, it will kill the BigQ threads, merge the difFile with sorted File as the new sorted File.

#### File Merge detail
There are some temporarily files.
1. difFile
2. mergeFile
3. sortedFile
- I merge difFile and sortedFile into mergeFile using a 2-way merge algorithm, get the smaller one each time
- Then I detele other files except the mergeFile, then I rename it into the right name.


![](./pictures/p2_new.png)

### implement of Binary Search

int DBFileSorted::GetNext (Record &fetchme, CNF &cnf, Record &literal):

I main 2 variables: lowerBound and higherBound, which are the possible minimum and maximum pageIndex
We scan all attributes in OrderMaker and CNF, find all the intersection attributes. Then for a LessThan operation, we find it's Upper Bound pageIndex. For a GreaterThan operation, we find it's Lower Bound pageIndex. As a result, the result must be in this range [lowerBound, higherBound], this will be a much smaller range compare to the original one. So, it will be much more efficient.

### test on CISE computer

When compiling in CISE Machine, you have to delete `-O2`, otherwise there might be problems. My problem is that I cannot open my file again after I close it.

After below operations, everything is OK

### gtest
we create 6 gtest cases and all of them passed.





## Result picture
### i) test1()
#### test.cc issues
as described here: https://ufl.instructure.com/courses/418634/discussion_topics/2913007
I find a problem in original code of project 2 part 2 code. Line 31 of test.cc (Links to an external site.), since it requires a CTRL+D, which is a "EOF", this make the consecutive "cin" not work. Attachment is the picture of endless while loop in Line 34 of test.cc (Links to an external site.)


As a result, I change sequence of test1(), move the CNF which requires a CTRL-D part as the last one to input, so the input.txt will be changed accordingly. test2() and test3() remian unchanged. The changed input.txt for test1() will be as below:
```
1
3
8
2
(c_phone)
```
![](./pictures/p2_1.png)


### ii) test2()
```
2
3
```
![](./pictures/p2_2.png)

### iii) test3()
```
3
3
(c_phone > '34-999-195-7029') AND (c_mktsegment = 'FURNITURE')
```
![](./pictures/p2_3.png)

### d) gtest
![](./pictures/p2_gtest.png)