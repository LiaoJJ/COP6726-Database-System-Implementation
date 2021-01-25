## This is my course project of COP6726 Database System Implementation

---
## proj1 Heap file implementation


#### How to

line 41 of makefile
```shell
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
```
should be change to
```shell
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
```

But still, there is segment fault