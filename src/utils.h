#ifndef UTILS_H
#define UTILS_H

#include "process.h"

typedef struct  {
    int* array;
    int size;
} DynamicArrayInt;


typedef struct  {
    char** array;    // Array of string pointers
    int size;     // Current size
} DynamicArrayString;

void write_res_to_csv(ResultStats** results, char** matric_nums, int num_inputs);
void calc_time_taken(char** inputs, int num_inputs, int total_num_records);
# endif

