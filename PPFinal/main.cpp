#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <algorithm>
#include <queue>
#include <sys/time.h>
#include <cstdio>
#include <math.h>
#include <pthread.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include "quick.h"
using namespace std;

#define THREAD_LEVEL 10
#define NUM_WAY 6

double total_sort_time = 0.0;
double total_read_time = 0.0;
double total_write_time = 0.0;
double total_run_time = 0.0;
int last_num = 0;
pthread_mutex_t _check_lock;

struct HeapNode{
    char element;
    int index;
};
struct compareH{
    bool operator()(const HeapNode lhs,const HeapNode rhs) const
    {
        return lhs.element > rhs.element;
    }
};

void generate_random_text_file(string filename,long int size){

    ofstream output(filename);
    long int curr_size=0;
    while(curr_size < size){
        char c = 'A' + rand()%26;
        output << c;
        curr_size++;
    }
    output.close();
    cout << "File done"<<endl;
}

string ToString(int val) {
    stringstream stream;
    stream << val;
    return stream.str();
}
int* charToInt(char *block, int size){
    int *temp = new int[size];
    for(int i = 0; i < size; i++){
        temp[i] = (int)block[i];
    }
    return temp;
}
void intToChar(int* temp, char* block, int size){
    for(int i = 0; i < size; i++)
        block[i] = temp[i];
}
//Sort phase parallel
void create_initial_runs(string inputfile,int run_size,int num_tempfile){
    char read = 'r',write='w';
    struct timeval start,end;
    FILE *input = fopen(inputfile.c_str(),&read);
    FILE *output_block_data[num_tempfile];

    for(int i = 0; i < num_tempfile; ++i){
        string sortedInputFileName = "./run2/output" + ToString(i) + ".txt";
        output_block_data[i] = fopen(sortedInputFileName.c_str(), &write);
    }

    bool more_input = true;
    int next_output_file = 0;
    int block_size, index;
    
    double secs;
    while(more_input){
        if(next_output_file >= num_tempfile)break;
        char* block = (char*)malloc(sizeof(char)*run_size);

        gettimeofday(&start, NULL);
        fread(block,sizeof(char),run_size,input);
        gettimeofday (&end, NULL);
        secs = ((double)end.tv_sec - (double)start.tv_sec) + ((double)end.tv_usec - (double)start.tv_usec)/1000000.0;
        total_read_time += secs;
        int *temp = charToInt(block, run_size);

        gettimeofday(&start , NULL);
        //sort(block, block+run_size);
        startQuicksort(temp, run_size);
        gettimeofday (&end, NULL);
        secs = ((double)end.tv_sec - (double)start.tv_sec) + ((double)end.tv_usec - (double)start.tv_usec)/1000000.0;
        total_sort_time += secs;

        gettimeofday(&start , NULL);
        intToChar(temp, block, run_size);
        fwrite(block, sizeof(char), run_size, output_block_data[next_output_file]);
        fclose(output_block_data[next_output_file]);
        gettimeofday (&end, NULL);
        secs = ((double)end.tv_sec - (double)start.tv_sec) + ((double)end.tv_usec - (double)start.tv_usec)/1000000.0;
        total_write_time += secs;

        next_output_file++;
        free(block);
    }
    fclose(input);
}
//Merge phase parallel
void *pmergesort(void *arg);
int domerge(int lists_to_merge[],int num_ways,int threadid);
queue<int> q;

void MergesphaseParallel(int num_tempfile){
    long i;
    for(i = 0; i < num_tempfile; ++i)
        q.push(i);

    const int processors = 4;

    pthread_t threads[processors];
    pthread_mutex_init(&_check_lock, NULL);

    for(i = 0; i < processors; ++i)
        pthread_create(&threads[i], NULL, pmergesort, (void *)i);
    for(i = 0; i < processors; ++i)
        pthread_join(threads[i], NULL);

    pthread_mutex_destroy(&_check_lock);
}

void *pmergesort(void *arg){
    long threadID = (long) arg;
    int num_ways = NUM_WAY;
    int final_file;
    
    while(q.size() > 1){
        if(q.size() < num_ways){
            int temp[q.size()];
            int i = 0;
            pthread_mutex_lock(&_check_lock);
            while(!q.empty()){
                temp[i] = q.front();
                cout << temp[i] << "A ";
                q.pop();
                i++;
            }
            cout << endl;
            pthread_mutex_unlock(&_check_lock);
            if(i){
                int tmp = domerge(temp, i, (int)threadID);
                q.push(tmp);
                final_file = tmp;
            }
        }
        else{
            int temp[num_ways];
            pthread_mutex_lock(&_check_lock);
            int i;
            for(i = 0;i<num_ways;++i){
                if(q.empty())
                    break;
                temp[i] = q.front();
                cout << temp[i] << "B ";
                q.pop();
            }
            cout << endl;
            pthread_mutex_unlock(&_check_lock);
            if(i){
                int tmp = domerge(temp, i, (int)threadID);
                cout << "File: " << tmp << endl;
                q.push(tmp);
                final_file = tmp;
            }
        }
    }
    cout << threadID << ", final file: " << final_file << endl;
    pthread_exit(NULL);
}

int domerge(int lists_to_merge[], int num_ways, int threadid){
    FILE* input[num_ways];
    int mynum;
    pthread_mutex_lock(&_check_lock);
    string outputFileName = "./run2/output" + ToString(last_num) + ".txt";
    mynum = last_num;
    last_num++;
    pthread_mutex_unlock(&_check_lock);
    FILE* temp = fopen(outputFileName.c_str(), "w");

    for(int i = 0; i < num_ways; ++i){
        string inputFileName = "./run2/output" + ToString(lists_to_merge[i]) + ".txt";
        input[i] = fopen(inputFileName.c_str(), "r");
    }
    
    priority_queue <HeapNode,vector<HeapNode>,compareH> h;
    HeapNode node[num_ways];
    int i;
    
    for(i = 0; i < num_ways; ++i){
        if(fscanf(input[i],"%c", &node[i].element) != 1)break;
        node[i].index = i;
        h.push(node[i]);
    }
    int count = 0;

    while(count != i){
        HeapNode root = h.top();
        h.pop();
        fprintf(temp, "%c", root.element);
        if(fscanf(input[root.index],"%c", &root.element) != 1){
            root.element = 125;
            count++;
        }
        h.push(root);
    }

    for(i = 0; i < num_ways; i++)
        fclose(input[i]);
    fclose(temp);
    cout << "mergefile" << outputFileName << endl;
    return mynum;
}
//Serial merge phase
void mergesort(int lists_to_merge[], int num_ways){
    FILE* input[num_ways];
    string outputFileName = "./run2/output" + ToString(last_num) + ".txt";
    last_num++;
    FILE* temp = fopen(outputFileName.c_str(), "w");

    for(int i = 0;i<num_ways;++i){
        string inputFileName = "./run2/output" + ToString(lists_to_merge[i]) + ".txt";
        input[i] = fopen(inputFileName.c_str(),"r");
    }
    
    priority_queue <HeapNode,vector<HeapNode>,compareH> h;
    HeapNode node[num_ways];
    int i;
    
    for(i = 0;i<num_ways;++i){
        if(fscanf(input[i],"%c",&node[i].element)!=1)break;
        node[i].index = i;
        h.push(node[i]);
    }
    int count = 0;

    while(count != i){
        HeapNode root = h.top();
        h.pop();
        fprintf(temp,"%c",root.element);
        if(fscanf(input[root.index],"%c",&root.element)!=1){
            root.element = 125;
            count ++;
        }
        h.push(root);
    }

    for(i = 0;i<num_ways;i++)
        fclose(input[i]);
    fclose(temp);
}
void mergephase(int num_ways,int num_tempfile){
    queue<int> q;
    for(int i =0;i<num_tempfile;++i)
        q.push(i);

    while(q.size()>1){
        if(q.size() < num_ways){
            int temp[q.size()];
            int i = 0;
            while(!q.empty()){
                temp[i] = q.front();
                cout << temp[i] << "A ";
                q.pop();
                i++;
            }
            cout << endl;
            mergesort(temp, sizeof(temp)/sizeof(*temp));
            //break;
        }
        else{
            int temp[num_ways];
            for(int i = 0;i<num_ways;++i){
                temp[i] = q.front();
                cout << temp[i] << "B ";
                q.pop();
            }
            cout << endl;
            mergesort(temp, sizeof(temp)/sizeof(*temp));
            q.push(last_num-1);
        }
    }
}

int main(int argc,char* argv[]){
    const long int inputSize = 1024*1000000;
    int pageSize = 1024*1000;
    if(argc > 1)
        pageSize = 1024*atoi(argv[1]);
    int num_tempfile = inputSize / pageSize;
    last_num = num_tempfile;
    cout << "Num temp file: " << num_tempfile << endl;

    struct timeval start,end;
    double secs;

    string inputfile = "input1GB.txt";
    string outputfile= "output.txt";
    char choice = 'n';
    cout << "Generate random text file or not? ";
    cin >> choice;
    if(choice == 'y')
        generate_random_text_file(inputfile,inputSize);

    gettimeofday(&start , NULL);
    create_initial_runs(inputfile,pageSize,num_tempfile);
    gettimeofday (&end, NULL);

    secs = ((double)end.tv_sec - (double)start.tv_sec) + ((double)end.tv_usec - (double)start.tv_usec)/1000000.0;
    cout << "Sort time: " << secs << "secs" << endl;
    cout << "Average write time: " << total_write_time / (double)num_tempfile << "secs" << endl;
    cout << "Average sort time: " << total_sort_time / (double)num_tempfile << "secs" << endl;
    cout << "Average read time: " << total_read_time / (double)num_tempfile << "secs" << endl;
    cout << "Total: " << total_read_time + total_sort_time + total_write_time << "secs" << endl;

    cout << "Finish create_initial_runs\nStart merging" << endl;
    gettimeofday(&start , NULL);
    //mergephase(NUM_WAY, num_tempfile);
    MergesphaseParallel(num_tempfile);
    gettimeofday (&end, NULL);

    secs = ((double)end.tv_sec - (double)start.tv_sec) + ((double)end.tv_usec - (double)start.tv_usec)/1000000.0;
    cout << "Merge time: " << secs << "secs" << endl;

    return 0;
}
