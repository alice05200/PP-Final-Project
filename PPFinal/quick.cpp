#include "quick.h"
//Source https://github.com/CrizMobius/ParallelQuicksort/blob/master/quicksort.c

const int g_sortCutoff = 100;   // cut off for forcing normal quicksort execution
int g_maxSize;                  // size of array
int g_arrayData[MAX_SIZE];      
int g_maxWorkers;               // number of workers

pthread_t workers[MAX_WORKERS];
struct WorkerData g_workerData[MAX_WORKERS];    // contains parameter data of each worker
pthread_attr_t g_attr;

double g_startTime;
double g_finalTime;

int g_activeWorkers = 0;
pthread_mutex_t g_lock;

void startQuicksort(int* block, int size){
    g_startTime = readTimer();
	initWorkerData(size);
    //generate(&block[0], &block[size]);
    pthread_attr_init(&g_attr);
    pthread_attr_setscope(&g_attr, PTHREAD_SCOPE_SYSTEM);
    pthread_attr_setdetachstate(&g_attr, PTHREAD_CREATE_JOINABLE);
    pthread_mutex_init(&g_lock, NULL);
    parallelQuicksort(&block[0], size, sizeof(int));
    g_finalTime = readTimer() - g_startTime;
    //printf("The execution time is %g sec\n", g_finalTime);
    pthread_mutex_destroy(&g_lock);
}

void initWorkerData(int size){
    for(int i = 0; i < g_maxWorkers; i++)
    {
        g_workerData[i].id = 0;
        g_workerData[i].start = 0;
        g_workerData[i].n = 0;
        g_workerData[i].size = 0;
    }
    g_maxSize = size;
    g_maxWorkers = MAX_WORKERS;
    g_activeWorkers = 0;
}

void generate(int * start, int * end){
    char read = 'r';
    string inputfile = "input1GB.txt";
    FILE *input = fopen(inputfile.c_str(),&read);
    char* block = (char*)malloc(sizeof(char)*MAX_SIZE);
    fread(block,sizeof(char),MAX_SIZE,input);
    int *temp = new int[MAX_SIZE];
    for(int i = 0; i < MAX_SIZE; i++){
        start[i] = (int)block[i];
    }
    fclose(input);
}

bool isSorted(int * start, int * end){
    start++;
    if(start == end)
        return true;
    while(start != end){
        if(*(start - 1) > *(start))
            return false;
        start++;  
    }
    return true;
}

int compare(const void * a, const void * b){
  return ( *(int*)a - *(int*)b );
}

/* worker entry point */
void * startThread(void * data){
    struct WorkerData * p = (struct WorkerData *) data;
    int id = p->id;
    #ifdef DEBUG
    printf("worker %d (pthread id %lu) has started\n", id, pthread_self());
    #endif
    parallelQuicksort(p->start, p->n, p->size);
    pthread_exit(0);
}
/* the recursive parallel quicksort function, takes an array of ints starting at the "start" pointer and contains "size" number of elements*/
void parallelQuicksort(int * start, int n, int size){
    /* if the array contains less elements than sort cut off, use normal quicksort instead */
    if(n < g_sortCutoff){
        qsort(start, n, sizeof(int), compare);
        return;
    }
    
    /* lock the mutex so we can check the worker count */
    pthread_mutex_lock(&g_lock);
    if(g_activeWorkers < g_maxWorkers){
        /* get worker id from counter */
        pthread_t worker = g_activeWorkers;
        g_activeWorkers++;
        pthread_mutex_unlock(&g_lock);
        
        /* get pivot */
        int pivotIndex = getPivot(start, n);

        int right = n - 1;
        /* Move pivot to end */
        swap(&start[pivotIndex], &start[right]);
        int storeIndex = 0;
        int i;
        for(i = 0; i < right; i++){
            if(start[i] < start[right]){
                swap(&start[i], &start[storeIndex]);
                storeIndex++;
            }
        }
        swap(&start[storeIndex], &start[right]);
        pivotIndex = storeIndex;
        
        /* split the current array into two sub arrays */
        g_workerData[worker].id = worker;
        g_workerData[worker].start = start + pivotIndex;
        g_workerData[worker].n = n - pivotIndex;
        g_workerData[worker].size = size;
        
        /* Create a separate thread to work on one of the sub arrays */
        pthread_create(&workers[worker], &g_attr, startThread, (void *) &g_workerData[worker]);
        
        /* Let this thread work on another sub array */
        parallelQuicksort(start, pivotIndex, size);
        
        /* join and wait for other thread to finish */
        void * status;
        int rc = pthread_join(workers[worker], &status);
        if(rc){
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
        #ifdef DEBUG
        printf("Main: completed join with worker %ld (pthread id %lu) having a status of %ld\n", 
            worker, workers[worker], (long)status);
        #endif
    }else{
        pthread_mutex_unlock(&g_lock);
        /* no available threads, do normal quicksort */
        qsort(start, n, sizeof(int), compare);
    }
}
/* select MAX_PIVOTS number of random pivots and choose one of them */
int getPivot(int * start, int n){
    /* if n is low skip selecting pivots */
    if(n < 2)
        return 0;
    struct Pivot pivots[MAX_PIVOTS];
    int maxPivots = (MAX_PIVOTS > n)? n : MAX_PIVOTS;
    int i;
    for(i = 0; i < maxPivots; i++){
        int index = rand() % n;
        pivots[i].index = index;
        pivots[i].value = start[index];
    }

    /* sort the pivots */
    qsort(&pivots[0], maxPivots, sizeof(struct Pivot), comparePivot);

    int pivot = pivots[maxPivots / 2].index;
    #ifdef DEBUG
    printf("pivot is %d\n", pivot);
    printf("pivot value is %d\n", start[pivot]);
    #endif
    return pivot;
}
/* compare function that sorts values in increasing order */
int comparePivot(const void * a, const void * b){
  return (((struct Pivot*)a)->value - ((struct Pivot*)b)->value);
}
/* swaps to integers in place by reference */
void swap(int * a, int * b){
    int tmp = *a;
    *a = *b;
    *b = tmp;
}
/* prints array from start to end */
void printArray(int * start, int * end){
    if(start == end)
        return;
    while(start != end)
    {
        printf("%c ", *start);
        start++;
    }
    printf("\n");
}
double readTimer(){
    static bool initialized = false;
    static struct timeval start;
    struct timeval end;
    if( !initialized ){
        gettimeofday( &start, NULL );
        initialized = true;
    }
    gettimeofday( &end, NULL );
    return (end.tv_sec - start.tv_sec) + 1.0e-6 * (end.tv_usec - start.tv_usec);
}