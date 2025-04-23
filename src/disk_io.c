#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "const.h"


int* get_IO_partition_int(int partition_num, int record_size, char* column_name){
    char filename[100];
    sprintf(filename, "./../database/%s.bin", column_name);

    FILE *file = fopen(filename, "r");
    if (!file) {
        printf("Error: Cannot open file %s\n", filename);
        return NULL;
    }

    long startAddress = partition_num * RECORDS_PER_PARTITION * record_size;
    fseek(file, startAddress, SEEK_SET);

    int* res = (int*)malloc(RECORDS_PER_PARTITION*sizeof(int));

    size_t items_read = fread(res, sizeof(int), RECORDS_PER_PARTITION , file);

    fclose(file);
    

    return res;
}



char** get_IO_partition_char(int partition_num, int record_size, char* column_name){
    char filename[100];
    sprintf(filename, "./../database/%s.bin", column_name);

    FILE *file = fopen(filename, "r");
    if (!file) {
        printf("Error: Cannot open file %s\n", filename);
        return NULL;
    }

    long startAddress = partition_num * RECORDS_PER_PARTITION * record_size;
    fseek(file, startAddress, SEEK_SET);

    char **res = (char**)malloc(RECORDS_PER_PARTITION * sizeof(char*));
    for (int i = 0; i < RECORDS_PER_PARTITION; i++) {
        // Allocate memory for each row
        res[i] = (char*)malloc(record_size * sizeof(char));
    }

    size_t items_read = fread(res, record_size, RECORDS_PER_PARTITION , file);

    fclose(file);
    
    return res;
}