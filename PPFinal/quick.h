#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>
#include <iostream>
#include <algorithm>
#include <fstream>
using namespace std;

#define MAX_SIZE 102400000 // 1GB
#define MAX_WORKERS 100
#define MAX_PIVOTS 10

struct WorkerData
{
    int id;
    int * start;
    int n;
    int size;
};
/* global variables */
struct Pivot
{
    int index;
    int value;
};

/* function prototypes */
void startQuicksort(int* block, int size);
void initWorkerData(int size);
void generate(int * start, int * end);
bool isSorted(int * start, int * end);
int compare(const void * a, const void * b);
void * startThread(void * data);
void parallelQuicksort(int * start, int n, int size);
int getPivot(int * start, int n);
int comparePivot(const void * a, const void * b);
void swap(int * a, int * b);
void printArray(int * start, int * end);
double readTimer();