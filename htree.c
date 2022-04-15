/*
Matthew Pham
mnp190003
CS/SE 3377 Sys. Prog. in Unix and Other Env.
Project 2: Multi-threaded Hash Tree
This program uses a Multi-threaded hash tree to compute
and return a hash value of a given file with a given tree height.
*/
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>     // for EINTR
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/mman.h>
#include "common.h"
#include "common_threads.h"
#include <pthread.h>

void pthread_exit(void *retval); // Exits a given thread and return a value through void* retval
void Usage(char*); // Print out the usage of the program and exit.                                            
uint32_t jenkins_one_at_a_time_hash(const uint8_t* , size_t); // Hashes a given string into a uint32_t number
void *tree(void *arg); // Function that the threads call to run
int getLength(uint32_t n); // Returns the number of digits in a given number n
char* threadHash(uint64_t offset); // Hashes and returns a char* for the hash value of the file at a given offset
char* concatenate(char* buf, char* leftCat, char* rightCat); // Concatenates 3 hash values at an internal thread and hashes and returns the concatenated value
char* leftmost_concatenate(char* at, char* left); // Concatenates 2 hash values at the leftmost thread in the tree
void err_exit(char* msg); // Exits program and calls perror

#define BSIZE 4096    // Current size of blocks in the program (4096 bytes)

int height;           // Stores the given max height of the tree 
int fd;               // Holds the file descriptor of the given file
uint64_t bytestoread; // Stores how many bytes each thread reads
char* addr;           // Holds the addr of the file which is mapped with mmap()

int
main(int argc, char** argv)
{   
    uint64_t nblocks;      // # of blocks in the entire file
    uint64_t threads;      // # of threads to be used (calculated by given tree height)
    struct stat sb;        // Holds data of file 
    char* finalHash;       // Used to hold final hash value of file
    
    // Input checking 
    if (argc != 3)
        Usage(argv[0]);
    // Opens file and checks for error 
    fd = open(argv[1], O_RDWR);
    if (fd == -1) 
        err_exit("opening file failed");
    
    // Retrieves stats of file 
    if (stat(argv[1], &sb) == -1) 
        err_exit("stat failed");
    
    // Calculates number of blocks
    nblocks = (sb.st_size / 4096);
    // If the file is too small it automatically sets blocks to 1
    if(nblocks == 0){
        nblocks = 1;
    }
    // Retrieves height and calculates # of threads with 2^(height + 1) 
    // then calculates bytes to read 
    if((height= atoi(argv[2])) < -1){
      printf("invalid height\n");
      exit(EXIT_FAILURE);
    }
    threads = 1 << (height + 1);
    bytestoread = nblocks/threads * BSIZE;
        
    printf("num Threads = %lu \n", (unsigned long)threads);
    printf("Blocks per Thread = %lu \n", (unsigned long)(nblocks/threads));
    
    // Maps entire file and returns a char* used to read file 
    if((addr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0)) == (void*) -1){
        err_exit("mmap failed");
    }
    // Sets arguments for the root thread of the tree 
    uint64_t treeArgs[2] = {height, 0};
    pthread_t p1;
    
    // Creates tree then tracks time taken for threads to run
    // Pthread_join of root thread returns the final hash value
    double start = GetTime();
    Pthread_create(&p1, NULL, tree, treeArgs);
    Pthread_join(p1, (void**)&finalHash);
    double end = GetTime();
    
    printf("hash value = %s\n", finalHash);
    printf("time taken = %f \n\n", (end - start));
    close(fd);
    
    // Unmaps the map since it is no longer used
    if(munmap(addr, sb.st_size) != 0)
        err_exit("unmapping failed");
    
    return EXIT_SUCCESS;
}

// Print out the usage of the program and exit.
void Usage(char* s){
      fprintf(stderr, "Usage: %s filename height \n", s);
      exit(EXIT_FAILURE);
}
// Hashes a given string into a uint32_t number
uint32_t jenkins_one_at_a_time_hash(const uint8_t* key, size_t length){
    size_t i = 0;
    uint32_t hash = 0;
    while (i != length) {
        hash += key[i++];
        hash += hash << 10;
        hash ^= hash >> 6;
    }
    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;
}
// A tree of threads is created so that each thread hashes a certain number of blocks
// in the file then returns a hash value which is then concatenated and hashed by its parent thread 
// until one final hash value is returned at the root 
void* tree(void* arg){
    int *vars = (int *)arg;                 // Retrieves current tree height and offset stored in arguments 
    int leftArgs[2] = {vars[0], vars[1]};   // Initializes arguments for the left child thread
    int rightArgs[2] = {vars[0], vars[1]};  // Initializes arguments for the right child thread
    
    char* leftCat;  // Used to hold returned hash value of left child 
    char* rightCat; // Used to hold returned hash value of right child 
    
    // Offset is set at 0 in a 1 thread tree
    // otherwise it is calculated at the thread index * bytestoread 
    uint64_t offset;
    if(vars[1] == -1)
      offset = 0;
    else
      offset = (uint64_t)vars[1] * bytestoread;
    
    // Retrieves the hash value of the data read at the given offset 
    char* buf = threadHash(offset);

    if(vars[0] <= 0){ // Checks if the current thread is a leaf thread
        // If it is the leftmost leaf thread the program creates 
        // a left thread child that returns its hash value to its parent 
        // and concatenates and hashes a new value to return
        if(vars[1] == (1 << height) - 1){ 
            // Offset is changed for the new left child 
            int newArgs[2] = {vars[0], vars[1]};
            pthread_t farLeft;
            newArgs[1] = 2 * vars[1] + 1; 
            
            // The left child thread is created and its hash value is returned
            Pthread_create(&farLeft, NULL, tree, newArgs);
            Pthread_join(farLeft, (void*)&leftCat);
            
            // The return value is then concatenated with the current thread's
            // hash value and is then hashed and returned through pthread_exit
            char* retVal = leftmost_concatenate(buf, leftCat);
            free(buf); // Memory allocated in buf is freed 
            pthread_exit((void*)retVal);
            return NULL;
        }
        // Leaf nodes return their hash value through pthread_exit
        pthread_exit((void*)buf);
        return NULL;
    }
    // Threads here are internal nodes 
    // Offset is changed for the left and right child
    // which are then created with Pthread_create
    pthread_t left;
    pthread_t right;
    leftArgs[0]--;
    rightArgs[0]--;

    leftArgs[1] = 2 * vars[1] + 1;
    Pthread_create(&left, NULL, tree, leftArgs);
    rightArgs[1] = 2 * vars[1] + 2;
    Pthread_create(&right, NULL, tree, rightArgs);
    // Pthread_join returns the hash values of the 2 child threads 
    Pthread_join(left, (void*)&leftCat);
    Pthread_join(right, (void*)&rightCat);
    
    // All 3 hash values are concatenated and hashed then returned
    char* retVal = concatenate(buf, leftCat, rightCat);
    
    // Memory allocated by the child threads are freed 
    free(leftCat);
    free(rightCat);
    
    // Thread exits with the returned hash 
    pthread_exit(retVal);
    return NULL;
}
// Returns the number of digits in a given number n
// This function recursively divides the n by 10 to 
// count the number of digits 
int getLength(uint32_t n){  
    static int counter=0; 
    if(n>0){  
      counter=counter+1;  
      return getLength(n/10);  
    }  
    else  
    return counter;  
}  
// Hashes and returns a char* for the hash value of the file at a given offset
char* threadHash(uint64_t thread_offset){
    // A pointer is set at the given offset in the file 
    char* hashInput = addr; 
    hashInput += thread_offset;
    
    // The length of the bytes to read is stored in length 
    // then used to hash that chunk of data in the file 
    uint64_t length = strnlen(hashInput, bytestoread);
    uint32_t hashResult = jenkins_one_at_a_time_hash((uint8_t*)hashInput, length);
    int retLength = getLength(hashResult) + 1;
    
    // The hash value is turned into a char* to be returned 
    char* result = (char*)malloc(sizeof(char) * retLength);
    if(sprintf(result, "%lu", (unsigned long) hashResult) < 0){
        err_exit("sprintf failed in threadHash");
    }
    return result;
}
// Concatenates 3 hash values at an internal thread and hashes and returns the concatenated value
char* concatenate(char* at, char* left, char* right){
    // The concatenated values are stored in result 
    char* result = (char*)malloc(sizeof(char) * 40);
    if(sprintf(result, "%s%s%s", at, left, right) < 0){
        err_exit("sprintf failed in concatenate");
    }
    
    // The concatenated values are then hashed 
    int retLength = strlen(at) + strlen(left) + strlen(right);
    uint32_t resultHash = jenkins_one_at_a_time_hash((unsigned char*)result, retLength);
    
    // Then the resulting hash is turned in a char* to be returned 
    if(sprintf(result, "%lu", (unsigned long) resultHash) < 0){
        err_exit("sprintf failed in concatenate");
    }
    return result;
}
// Concatenates 2 hash values at the leftmost thread in the tree
char* leftmost_concatenate(char* at, char* left){
    // The concatenated values are stored in result 
    char* result = (char*)malloc(sizeof(char) * 25);
    if(sprintf(result, "%s%s", at, left) < 0){
        err_exit("sprintf failed in concatenate");
    }
    
    // The concatenated values are then hashed 
    int retLength = strlen(at) + strlen(left);
    uint32_t resultHash = jenkins_one_at_a_time_hash((unsigned char*)result, retLength);
    
    // Then the resulting hash is turned in a char* to be returned 
    if(sprintf(result, "%lu", (unsigned long) resultHash) < 0){
        err_exit("sprintf failed");
    }
    return result;
}
// Exits program and calls perror
void err_exit(char* msg){
    perror(msg);
    exit(EXIT_FAILURE);
}

