#include "global.h"
#include "const.h"
#include <stdio.h>
#include <stdlib.h>


// Declare the global variable
GlobalIOTracer* globalIO = NULL;

// Create initialization function
void initGlobalIOTracer() {
    globalIO = (GlobalIOTracer*)malloc(sizeof(GlobalIOTracer));
    if (globalIO == NULL) {
        fprintf(stderr, "Failed to allocate memory for globalIO\n");
        exit(1);
    }

    globalIO->is_disabled = 0;
}

void disableGlobalIOTracer(){
    globalIO->is_disabled = 1;
}

void enableGlobalIOTracer(){
    globalIO->is_disabled = 0;
}

void resetGlobalIOTracer(){
    globalIO->num_IO = 0;
}

void addGlobalIO(){
    if (globalIO->is_disabled == 0){
        globalIO->num_IO +=1;
    }
}

// Create cleanup function
void cleanupGlobalIO() {
    if (globalIO != NULL) {
        free(globalIO);
        globalIO = NULL;
    }
}


void print_globalIO( int j){
    printf("%-30s: %ld I/Os\n", 
        j==0 ? "Normal Scan" : 
        j==1 ? "Zone Map" : 
        j==2 ? "Shared Scan" : 
        j==3 ? "Zone Map + Shared Scan" : 
        "Undefined scan method", 
        globalIO->num_IO);
}