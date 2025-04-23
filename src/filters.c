
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "const.h"
#include "filters.h"
#include "utils.h"
#include "disk_io.h"
#include "functions.h"



// Function that takes scan type as input
DynamicArrayInt* filter_scan(ScanType scan_type, char* matric_num, int total_num_records) {

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


    switch (scan_type) {
        case NORMAL_SCAN:
            // output_lines = get_lines_filter_year(input_lines, total_num_records, YEAR_SIZE,  year); 
            // output_lines = get_lines_filter_month(output_lines->array,output_lines->size, MONTH_SIZE, month);
            // output_lines = get_lines_filter_town(output_lines->array, output_lines->size, TOWN_SIZE,  town); 
            // output_lines = get_lines_filter_area(output_lines->array, output_lines->size, FLOOR_AREA_SQM_SIZE, area);


            output_lines = get_lines_column("year", input_lines, total_num_records,  year); 
            output_lines = get_lines_column("month", output_lines->array,output_lines->size, month);
            output_lines = get_lines_column("town", output_lines->array, output_lines->size,  town); 
            output_lines = get_lines_column("floor_area_sqm", output_lines->array, output_lines->size, area);
            break;
            
        case ZONE_MAP_SCAN:





            printf("Performing zone map scan\n");
            // Zone map scan implementation
            break;
            
        case SHARED_SCAN:
            printf("Performing shared scan\n");
            // Shared scan implementation
            break;
            
        case ZONE_MAP_SHARED_SCAN:
            printf("Performing zone map + shared scan\n");
            // Combined zone map and shared scan implementation
            break;
            
        default:
            printf("Unknown scan type\n");
            break;
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
            arr = get_IO_partition_int(block_num,  record_size, column_name);
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



DynamicArrayInt* get_lines_filter_year(int* input_lines, int num_input_lines, int record_size, int filter_val){
    int last_partition_num = -1;
    int partition_num = -1;
    int count_input_line = 0;
    char* column_name = "year";

    int *output_lines = (int *)malloc(sizeof(int) * num_input_lines);
    int count_output_line = 0;

    while (count_input_line < num_input_lines){

        int partition_num_required = input_lines[count_input_line] / RECORDS_PER_PARTITION;
        // if need to load a new partition
        if (partition_num_required > last_partition_num){
            last_partition_num = partition_num_required;

            // load the partition (cost one I/O)

            int *arr = get_IO_partition_int(partition_num_required,  record_size, column_name);

            // index number relative to a partition (0-999)
            int local_index;

            while ((count_input_line < num_input_lines) && (input_lines[count_input_line]/RECORDS_PER_PARTITION == partition_num_required)){
                local_index = input_lines[count_input_line] % RECORDS_PER_PARTITION;
                if (arr[local_index] == filter_val){
                    output_lines[count_output_line] = input_lines[count_input_line];
                    count_output_line ++;
                }
                count_input_line ++;
            }
        }
        else {
            count_input_line ++;
        }
    }

    DynamicArrayInt* output_lines_ = (DynamicArrayInt*)malloc(sizeof(DynamicArrayInt));
    output_lines_->array = output_lines;
    output_lines_->size = count_output_line;

    return output_lines_;
}

DynamicArrayInt* get_lines_filter_month(int* input_lines, int num_input_lines, int record_size, int filter_val){
    int last_partition_num = -1;
    int partition_num = -1;
    int count_input_line = 0;
    char* column_name = "month";

    int *output_lines = (int *)malloc(sizeof(int) * num_input_lines);
    int count_output_line = 0;

    while (count_input_line < num_input_lines){

        int partition_num_required = input_lines[count_input_line] / RECORDS_PER_PARTITION;
        // if need to load a new partition
        if (partition_num_required > last_partition_num){
            last_partition_num = partition_num_required;

            // load the partition (cost one I/O)
            int *arr = get_IO_partition_int(partition_num_required,  record_size, column_name);

            // index number relative to a partition (0-999)
            int local_index;

            while ((count_input_line < num_input_lines) && (input_lines[count_input_line]/RECORDS_PER_PARTITION == partition_num_required)){
                local_index = input_lines[count_input_line] % RECORDS_PER_PARTITION;
                if ((arr[local_index] == filter_val) || (arr[local_index] == filter_val+1)){
                    output_lines[count_output_line] = input_lines[count_input_line];
                    count_output_line ++;
                }
                count_input_line ++;
            }
        }
        else {
            count_input_line ++;
        }
    }

    DynamicArrayInt* output_lines_ = (DynamicArrayInt*)malloc(sizeof(DynamicArrayInt));
    output_lines_->array = output_lines;
    output_lines_->size = count_output_line;



    return output_lines_;
}



DynamicArrayInt* get_lines_filter_town(int* input_lines, int num_input_lines, int record_size, int filter_val){ // filter_val is town integer mapping value
    int last_partition_num = -1;
    int partition_num = -1;
    int count_input_line = 0;
    char* column_name = "town";


    int *output_lines = (int *)malloc(sizeof(int) * num_input_lines);
    int count_output_line = 0;

    while (count_input_line < num_input_lines){

        int partition_num_required = input_lines[count_input_line] / RECORDS_PER_PARTITION;
        // if need to load a new partition
        if (partition_num_required > last_partition_num){
            last_partition_num = partition_num_required;

            // load the partition (cost one I/O)
            int *arr = get_IO_partition_int(partition_num_required,  record_size, column_name);

            // index number relative to a partition (0-999)
            int local_index;

            while ((count_input_line < num_input_lines) && (input_lines[count_input_line]/RECORDS_PER_PARTITION == partition_num_required)){
                local_index = input_lines[count_input_line] % RECORDS_PER_PARTITION;
                if (arr[local_index] == filter_val){
                    output_lines[count_output_line] = input_lines[count_input_line];
                    count_output_line ++;
                }
                count_input_line ++;
            }
        }
        else {
            count_input_line ++;
        }
    }

    DynamicArrayInt* output_lines_ = (DynamicArrayInt*)malloc(sizeof(DynamicArrayInt));
    output_lines_->array = output_lines;
    output_lines_->size = count_output_line;

    return output_lines_;
}


DynamicArrayInt* get_lines_filter_area(int* input_lines, int num_input_lines, int record_size, int filter_val){
    int last_partition_num = -1;
    int partition_num = -1;
    int count_input_line = 0;
    char* column_name = "floor_area_sqm";

    int *output_lines = (int *)malloc(sizeof(int) * num_input_lines);
    int count_output_line = 0;
    int partition_num_required = -1;

    while (count_input_line < num_input_lines){

        partition_num_required = input_lines[count_input_line] / RECORDS_PER_PARTITION;
        // if need to load a new partition
        if (partition_num_required > last_partition_num){
            last_partition_num = partition_num_required;

            // load the partition (cost one I/O)
            int *arr = get_IO_partition_int(partition_num_required,  record_size, column_name);

            // index number relative to a partition (0-999)
            int local_index;

            while ((count_input_line < num_input_lines) && (input_lines[count_input_line]/RECORDS_PER_PARTITION == partition_num_required)){
                local_index = input_lines[count_input_line] % RECORDS_PER_PARTITION;
                if (arr[local_index] >= filter_val){
                    output_lines[count_output_line] = input_lines[count_input_line];
                    count_output_line ++;
                }
                count_input_line ++;
            }
        }
        else {
            count_input_line ++;
        }
    }

    DynamicArrayInt* output_lines_ = (DynamicArrayInt*)malloc(sizeof(DynamicArrayInt));
    output_lines_->array = output_lines;
    output_lines_->size = count_output_line;

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
            arr = get_IO_partition_int(block_num,  record_size, column_name);
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

