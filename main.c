#include "functions.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>



int main(void) {

    char *inputFile = "./data/ResalePricesSingapore.csv";
    int total_num_records = 222833;

    ColumnMetaData columnsMetaData[11];
    storeColumnOrientedData(inputFile,total_num_records,columnsMetaData);
    int lines[1][2] = {{0,MAX_NUM_RECORDS}};
    printf("\ndone part 1\n");


    // filter for year
    int year_record_size = columnsMetaData[0].record_size; // 4
    int num_records_per_partition = columnsMetaData[0].num_records_per_partition; // 1000

    int *year_lines =  filter_year(lines, 1, 2014, 4, 4*1000); //  return all indexs where year = 2014
    int i = 0;
    int line = 0;
    printf("year filter result start_line = %d\n",  year_lines[i]);
    while (year_lines[i]!=-1){
        line = year_lines[i];
        i++;
    }
    printf("year filter result end_line = %d\n",  year_lines[i-1]);
    return 0;


    // filter for month

}
