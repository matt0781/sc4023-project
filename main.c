#include "functions.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>



int main(void) {

    char *inputFile = "./data/ResalePricesSingapore.csv";
    int total_num_records = 222833;

    ColumnMetaData columnsMetaData[11];
    storeColumnOrientedData(inputFile,total_num_records,columnsMetaData);

    printf("\ndone part 1\n");



    ////// filter for year

    // define arguments
    char* column_name = "year";
    int lines[TOTAL_NUM_RECORDS];
    int filter_val = 2015;
    for (int i=0; i<TOTAL_NUM_RECORDS;i++){
        lines[i] = i;
    }

    // execute the filter
    int* res_year = get_indexes_filter_int(column_name, lines, TOTAL_NUM_RECORDS, sizeof(int), filter_val);


    // print result
    int idx_res_year=0;
    printf("start index year filter = %d\n", res_year[idx_res_year]);
    while (res_year[idx_res_year] !=-1){
        idx_res_year+=1;
    }
    printf("end index year filter = %d\n", res_year[idx_res_year-1]);



    ////// filter for month

    // define arguments
    char* column_name2 = "month";
    int filter_val_month = 7;
   

    // execute the filter
    int* res_month = get_indexes_filter_int(column_name2, res_year, idx_res_year, sizeof(int), filter_val_month);


    // print result
    int idx_res_month=0;
    printf("start index month filter = %d\n", res_month[idx_res_month]);
    while (res_month[idx_res_month] !=-1){
        idx_res_month+=1;
    }
    printf("end index month filter = %d\n", res_month[idx_res_month-1]);
    return 0;




}
