#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "const.h"
#include "load_db.h"
#include "disk_io.h"




ColumnMetaData* storeColumnOrientedData(char *inputCsvFile, int total_num_records) {
    FILE *csv = fopen(inputCsvFile, "r");
    if (!csv) {
        perror("Error opening CSV file");
        return 0;
    }
    
    // Read header line to get column names
    char headerLine[1024];
    if (!fgets(headerLine, sizeof(headerLine), csv)) {
        fprintf(stderr, "Empty CSV file.\n");
        fclose(csv);
        return 0;
    }
    
    
    int count = 0;
    char line[1024];
    char *fields[10];
    Buffer* buffer = (Buffer*)malloc(sizeof(Buffer));
    
    if (buffer == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        return NULL;
    }
    
    // Compression that convert string to const.c file
    // note that the compression for town is hard-coded in const.h
    CompressionMap *compression_flat_type = (CompressionMap*)malloc(sizeof(CompressionMap*));
    CompressionMap *compression_block = (CompressionMap*)malloc(sizeof(CompressionMap*));
    CompressionMap *compression_street_name = (CompressionMap*)malloc(sizeof(CompressionMap*));
    CompressionMap *compression_storey_range = (CompressionMap*)malloc(sizeof(CompressionMap*));
    CompressionMap *compression_flat_model = (CompressionMap*)malloc(sizeof(CompressionMap*));


    // Set the max_count and count
    compression_flat_type->max_count = 30; compression_flat_type->count = 0;
    compression_block->max_count = 3000; compression_block->count = 0;
    compression_street_name->max_count = 1000; compression_street_name->count = 0;
    compression_storey_range->max_count = 50; compression_storey_range->count = 0;
    compression_flat_model->max_count = 50; compression_flat_model->count = 0;


    // Allocate memory for the string pointer arrays
    compression_flat_type->map = (char**)malloc(compression_flat_type->max_count * sizeof(char*));
    compression_block->map = (char**)malloc(compression_block->max_count * sizeof(char*));
    compression_street_name->map = (char**)malloc(compression_street_name->max_count * sizeof(char*));
    compression_storey_range->map = (char**)malloc(compression_storey_range->max_count * sizeof(char*));
    compression_flat_model->map = (char**)malloc(compression_flat_model->max_count * sizeof(char*));



    while (count < total_num_records){
        if (fgets(line, 1024, csv)){
            // load a record of csv into fields
            parseCSVLine(line, fields, 10);

            char year_str[5]; // 4 digits + null terminator
            char month_str[3]; // 2 digits + null terminator
            strncpy(year_str, fields[0], 4);
            year_str[4] = '\0';
            strncpy(month_str, &fields[0][5], 2);
            month_str[2] = '\0';

            buffer->year[count] = atoi(year_str);
            buffer->month[count] = atoi(month_str);
            buffer->town[count] = get_digit_by_town(fields[1]);
            buffer->flat_type[count] = get_compression_key(compression_flat_type, fields[2]);
            buffer->block[count] = get_compression_key(compression_block, fields[3]);
            buffer->street_name[count] = get_compression_key(compression_street_name, fields[4]);
            buffer->storey_range[count] = get_compression_key(compression_storey_range, fields[5]);
            buffer->flat_model[count] = get_compression_key(compression_flat_model, fields[7]);
            buffer->floor_area_sqm[count] = atoi(fields[6]);
            buffer->lease_commence_date[count] = atoi(fields[8]);
            buffer->resale_price[count] = atoi(fields[9]);

            count ++;
            
        }
    }

    // save_compression_map the data into db
    save_buffer_to_binary_files(buffer, count);

    // save the compression map into db
    save_compression_map(compression_flat_type, "flat_type");
    save_compression_map(compression_block, "block");
    save_compression_map(compression_street_name, "street_name");
    save_compression_map(compression_storey_range, "storey_range");
    save_compression_map(compression_flat_model, "flat_model");



    ColumnMetaData* columnsMetaData = malloc(11 * sizeof(ColumnMetaData));

    //ColumnMetaData columnsMetaData[11];
    for (int i=0; i<11; i++){
        create_metadata(buffer, &columnsMetaData[i], total_num_records, i);
    }


    return columnsMetaData;
}





void create_metadata(Buffer *buffer, ColumnMetaData *columnMetaData, int num_records, int column_num){
    columnMetaData->num_records_total = num_records;
    columnMetaData->record_size = record_size_mapping[column_num];
    columnMetaData->num_records_per_partition = RECORDS_PER_PARTITION;
    columnMetaData->num_partitions = (num_records+RECORDS_PER_PARTITION-1)/RECORDS_PER_PARTITION;
    columnMetaData->column_name = strdup(column_name_mapping[column_num]);

    if (is_int_column[column_num]){
        columnMetaData->max_values = (int *)malloc(columnMetaData->num_partitions * sizeof(int));
        columnMetaData->min_values = (int *)malloc(columnMetaData->num_partitions * sizeof(int));

        int np = columnMetaData->num_partitions;

        for (int i=0; i<np; i++){
            int max = -10000000;
            int min = 10000000;

            for(int j=0; j<RECORDS_PER_PARTITION && i*RECORDS_PER_PARTITION + j < num_records; j++ ){

                int index = i*RECORDS_PER_PARTITION + j;
                int val = 0;
                switch(column_num){
                    case 0: val = buffer->year[index]; break;
                    case 1: val = buffer->month[index]; break;
                    case 7: val = buffer->floor_area_sqm[index]; break;
                    case 9: val = buffer->lease_commence_date[index]; break;
                    case 10: val = buffer->resale_price[index];
                }

                max = val > max? val: max;
                min = val < min? val: min;
            }


            columnMetaData->max_values[i] = max;
            columnMetaData->min_values[i] = min;

            
        }


    }
}





void parseCSVLine(char* line, char* fields[], int numFields) {
    char* token;
    int fieldIndex = 0;
    
    // Get the first token
    token = strtok(line, ",");
    
    // Continue getting tokens until we run out or fill all fields
    while (token != NULL && fieldIndex < numFields) {
        fields[fieldIndex] = token;
        fieldIndex++;
        token = strtok(NULL, ",\n");
    }
    
    // Fill remaining fields with NULL if we don't have enough tokens
    while (fieldIndex < numFields) {
        fields[fieldIndex] = NULL;
        fieldIndex++;
    }
}




int get_digit_by_town(const char* town_name) {
    for (int i = 0; i < 26; i++) {
        if (strcmp(town_mapping[i], town_name) == 0) {
            return i;
        }
    }
    return -1; 
}




int get_column_id_by_name(char* column_name) {
    for (int i = 0; i < 11; i++) {
        if (strcmp(column_name_mapping[i], column_name) == 0) {
            return i;
        }
    }
    return -1;
}




int get_compression_key(CompressionMap* compression_map, char* compression_val){
    if (compression_map->map == NULL) {
        return -1;  
    }
    int count = compression_map->count;

    if (count == 0){
        compression_map->map[0] = strdup(compression_val);
        compression_map->count ++;
        return 0;
    }

    for (int i=0; i< count; i++){
        if (strcmp(compression_map->map[i], compression_val) == 0){            
            return i;
        }
    }

    compression_map->map[count] = strdup(compression_val);
    compression_map->count++;
    return count;
}