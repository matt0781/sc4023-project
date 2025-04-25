
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "const.h"
#include "filters.h"
#include "utils.h"
#include "disk_io.h"
#include "load_db.h"





// Function that takes scan type as input
DynamicArrayInt* filter_scan(Optimizer optimizer, char* matric_num, int total_num_records, ColumnMetaData* columnMetaData) {

    int year =  (matric_num[7]-'0') <= 3 ? 2020 + (matric_num[7]-'0') : 2010 + (matric_num[7]-'0') ; // year 20xx
    int month = matric_num[6] == '0' ? 10: (matric_num[6]-'0'); // starting month 

    int town = matric_num[5]-'0'; // town is stored as int in database, we keep the town_mapping (int, town_string) in const.h
    int area = 80; // area max limit = 80

    int *input_lines = (int*)malloc(sizeof(int)*total_num_records); for (int i=0; i<total_num_records; i++) input_lines[i]=i;

    int *year_lines;
    int *month_lines;
    int *town_lines;
    int *area_lines;

    int size = 0;


    DynamicArrayInt *output_lines; 


    if ((optimizer == NORMAL) || (optimizer == SHARED_SCAN)) {

        output_lines = get_lines_column("year", input_lines, total_num_records,  year); 
        output_lines = get_lines_column("month", output_lines->array,output_lines->size, month);
        output_lines = get_lines_column("town", output_lines->array, output_lines->size,  town); 
        output_lines = get_lines_column("floor_area_sqm", output_lines->array, output_lines->size, area);
    }

    else if ((optimizer == ZONE_MAP) || (optimizer == ZONE_MAP_SHARED_SCAN)){
         // Zone map scan implementation
         int total_num_blocks = columnMetaData[0].num_partitions; // this is the number of partitions (block) for the year column. This number is same for year,month,area columns because we standardize it, they have the same record size (4bytes int).
         int *valid_blocks = (int*)malloc(sizeof(int)*total_num_blocks);
         int count_valid_block = 0;

         int* years_min = columnMetaData[0].min_values;
         int* years_max = columnMetaData[0].max_values;
         int* months_min = columnMetaData[1].min_values;
         int* months_max = columnMetaData[1].max_values;
         int* areas_min = columnMetaData[7].min_values;
         int* areas_max = columnMetaData[7].max_values;

         for (int i=0; i<total_num_blocks; i++){
             if ((year < years_min[i]) | (year > years_max[i])) continue;
             if ((month + 1 < months_min[i]) | (month > months_max[i])) continue;
             if (area > areas_max[i]) continue;
             valid_blocks[count_valid_block] = i;
             count_valid_block ++;
         }

         output_lines = get_lines_column_zone_map("year", input_lines, total_num_records,  year, valid_blocks, count_valid_block); 
         output_lines = get_lines_column_zone_map("month", output_lines->array,output_lines->size, month, valid_blocks, count_valid_block);
         output_lines = get_lines_column_zone_map("town", output_lines->array, output_lines->size,  town, valid_blocks, count_valid_block); 
         output_lines = get_lines_column_zone_map("floor_area_sqm", output_lines->array, output_lines->size, area, valid_blocks, count_valid_block);

    }
            

    return output_lines;

}


DynamicArrayInt* get_lines_column(char* column_name, int* input_lines, int num_input_lines, int filter_val){

    int record_size = record_size_mapping[get_column_id_by_name(column_name)];
    int column_id = get_column_id_by_name(column_name);

    int *output_lines = (int *)malloc(sizeof(int) * num_input_lines);
    int count_output = 0;
    int count_input = 0;

    int last_block_num = -1;
    int block_num = -1;
    int local_index = 0;

    int* arr;


    while (count_input <num_input_lines){
        block_num = input_lines[count_input] / RECORDS_PER_PARTITION;

        // load a new block data
        if (block_num > last_block_num){
            arr = get_IO_int(block_num,  record_size, column_name);
        }

        // while still needing to access the same block
        while ((count_input < num_input_lines) && (input_lines[count_input]/RECORDS_PER_PARTITION == block_num)){
            local_index = input_lines[count_input] % RECORDS_PER_PARTITION;

            switch(column_id){
                case 0:  // year
                    if (arr[local_index] == filter_val){
                        output_lines[count_output] = input_lines[count_input];
                        count_output ++;
                    }
                    break;
                case 1: // month
                    if ((arr[local_index] == filter_val) || (arr[local_index] == filter_val+1)){
                        output_lines[count_output] = input_lines[count_input];
                        count_output ++;
                    }
                    break;
                case 2:  // town
                    if (arr[local_index] == filter_val){
                        output_lines[count_output] = input_lines[count_input];
                        count_output ++;
                    }
                    break;
                case 7: // area
                    if (arr[local_index] >= filter_val){
                        output_lines[count_output] = input_lines[count_input];
                        count_output ++;
                    }
                    break;
            }

            count_input ++;
        } 
    }

    DynamicArrayInt* output_lines_ = (DynamicArrayInt*)malloc(sizeof(DynamicArrayInt));
    output_lines_->array = output_lines;
    output_lines_->size = count_output;

    return output_lines_;
}



int* get_values_column(char* column_name, int* input_lines, int num_input_lines){
    
    int record_size = record_size_mapping[get_column_id_by_name(column_name)];

    int *output_values = (int *)malloc(sizeof(int) * num_input_lines);
    int count = 0;

    int last_block_num = -1;
    int block_num = -1;
    int local_index = 0;

    int* arr;

    while (count <num_input_lines){
        block_num = input_lines[count] / RECORDS_PER_PARTITION;

        // load a new block data
        if (block_num > last_block_num){
            arr = get_IO_int(block_num,  record_size, column_name);
        }

        // while still needing to access the same block
        while ((count < num_input_lines) && (input_lines[count]/RECORDS_PER_PARTITION == block_num)){
            local_index = input_lines[count] % RECORDS_PER_PARTITION;
            output_values[count] = arr[local_index];
            count ++;
        } 
    }

    return output_values;
}


DynamicArrayInt* get_lines_column_zone_map(char* column_name, int* input_lines, int num_input_lines, int filter_val, int* valid_blocks, int num_valid_blocks){

    int record_size = record_size_mapping[get_column_id_by_name(column_name)];
    int column_id = get_column_id_by_name(column_name);

    int *output_lines = (int *)malloc(sizeof(int) * num_input_lines);
    int count_output = 0;
    int count_input = 0;

    int last_block_num = -1;
    int block_num = -1;
    int local_index = 0;

    int* arr;

    int count_valid_block = 0;


    while ((count_input <num_input_lines) && (count_valid_block<num_valid_blocks)){
        block_num = input_lines[count_input] / RECORDS_PER_PARTITION;
        if (block_num < valid_blocks[count_valid_block]){
            count_input++;
            continue;
        }else if (block_num > valid_blocks[count_valid_block]){
            count_valid_block ++;
            continue;
        }


        // load a new block data
        if (block_num > last_block_num){
            arr = get_IO_int(block_num,  record_size, column_name);
        }

        // while still needing to access the same block
        while ((count_input < num_input_lines) && (input_lines[count_input]/RECORDS_PER_PARTITION == block_num)){
            local_index = input_lines[count_input] % RECORDS_PER_PARTITION;

            switch(column_id){
                case 0:  // year
                    if (arr[local_index] == filter_val){
                        output_lines[count_output] = input_lines[count_input];
                        count_output ++;
                    }
                    break;
                case 1: // month
                    if ((arr[local_index] == filter_val) || (arr[local_index] == filter_val+1)){
                        output_lines[count_output] = input_lines[count_input];
                        count_output ++;
                    }
                    break;
                case 2:  // town
                    if (arr[local_index] == filter_val){
                        output_lines[count_output] = input_lines[count_input];
                        count_output ++;
                    }
                    break;
                case 7: // area
                    if (arr[local_index] >= filter_val){
                        output_lines[count_output] = input_lines[count_input];
                        count_output ++;
                    }
                    break;
            }

            count_input ++;
        } 
    }

    DynamicArrayInt* output_lines_ = (DynamicArrayInt*)malloc(sizeof(DynamicArrayInt));
    output_lines_->array = output_lines;
    output_lines_->size = count_output;

    return output_lines_;
}