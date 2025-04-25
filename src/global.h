// global.h
#ifndef GLOBAL_H
#define GLOBAL_H
#include "const.h"

typedef struct {
    long num_IO; 
    int is_disabled;
} GlobalIOTracer;

void initGlobalIOTracer();
void addGlobalIO();
void disableGlobalIOTracer();
void resetGlobalIOTracer();
void print_globalIO( int j);
#endif


