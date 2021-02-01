# This is my course project of COP6726 Database System Implementation

---
# [proj1 Heap file implementation](/proj1/P1)

## Abstract
A Heap File System consists of many pages, all of them will be stored in the disk except one buffer page.

The writing will only be allowed at the end of the file. O(1)

The reading can only start from the beginning of this file.
- when record is in file, O(n/2)
- when record is not in the file, O(n)

For editing, it will requires for a scan of the file, and the time complexity will be the same with reading.

## Implementation
For writing, new records will be stored temporarily at the the buffer page. when this buffer page is full, it will be write into disk/file.
For reading, a page of records will be read from disk each time, and there will be a pointer which indicating the current position of records, and this pointer could be moved to the next record.













# Common Mistakes in C++

## Directory problem
remember to add `/` at the last of a directory:
`const char *tpch_dir ="/Users/ljj/Documents/Courses/0 courses/DBSI/projects/tpch-dbgen/";` instead of `const char *tpch_dir ="/Users/ljj/Documents/Courses/0 courses/DBSI/projects/tpch-dbgen";`

## How to compile

Refer to Assignment page for generating date
```shell
mkdir ~/git; cd git; git clone https://github.com/electrum/tpch-dbgen.git 
make
./dbgen -s 0.01
```

## PLAN A
line 41 of makefile
```shell
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
```
should be change to
```shell
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
```

But still, there is segment fault

## PLAN B connect to CISE thunder computer
connect to VPN of UFL
, `ssh liao@thunder.cise.ufl.edu`, also segment fault

# Bugs already known
While printing, Double variable will only display some part of it. However, this doesn't influence the accuracy of query.
So, please refer to the number in the *.tbl files instead of print out result.