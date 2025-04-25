#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "const.h"
#include "functions.h"
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



// Function to save each attribute to separate binary files
void save_buffer_to_binary_files(Buffer* buffer, int num_records) {
    FILE *fp;

    // Check if database directory exists, create if it doesn't
    #ifdef _WIN32
        // Windows
        if (system("if not exist .\\..\\database mkdir .\\database") != 0) {
            printf("Failed to create database directory\n");
        }
    #else
        // Unix/Linux/macOS
        if (system("mkdir -p ./../database") != 0) {
            printf("Failed to create database directory\n");
        }
    #endif

    // Save year data
    fp = fopen("./../database/year.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->year, YEAR_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save month data
    fp = fopen("./../database/month.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->month, MONTH_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save town data
    fp = fopen("./../database/town.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->town, TOWN_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save flat_type data
    fp = fopen("./../database/flat_type.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->flat_type, FLAT_TYPE_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save block data
    fp = fopen("./../database/block.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->block, BLOCK_ATTR_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save street_name data
    fp = fopen("./../database/street_name.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->street_name, STREET_NAME_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save storey_range data
    fp = fopen("./../database/storey_range.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->storey_range, STOREY_RANGE_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save floor_area_sqm data
    fp = fopen("./../database/floor_area_sqm.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->floor_area_sqm, FLOOR_AREA_SQM_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save flat_model data
    fp = fopen("./../database/flat_model.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->flat_model, FLAT_MODEL_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save lease_commence_date data
    fp = fopen("./../database/lease_commence_date.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->lease_commence_date, LEASE_COMMENCE_DATE_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save resale_price data
    fp = fopen("./../database/resale_price.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->resale_price, RESALE_PRICE_SIZE, num_records, fp);
        fclose(fp);
    }
    
    printf("Data is succesfully written into respective column binary files in ./../database directory.\n");
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




void save_compression_map(CompressionMap* compression_map, char* filename) {
    if (compression_map == NULL || compression_map->map == NULL || filename == NULL) {
        printf("ERROR: NULL pointer detected\n");
        return;
    }

    #ifdef _WIN32
        // Windows
        if (system("if not exist .\\..\\database mkdir .\\database") != 0) {
            printf("Failed to create database directory\n");
        }
    #else
        // Unix/Linux/macOS
        if (system("mkdir -p ./../database") != 0) {
            printf("Failed to create database directory\n");
        }
    #endif
    
    char filename_[100];
    sprintf(filename_, "./../database/compression_map_%s.txt", filename);

    // Open file in write mode, not read mode
    FILE *file = fopen(filename_, "w");
    if (!file) {
        printf("Error: Cannot open file %s for writing\n", filename_);
        return;
    }
    
    int count = compression_map->count;
    
    // Write each entry in text format
    for (int i = 0; i < count; i++) {
        if (compression_map->map[i] != NULL) {
            // Write in format: "index value"
            fprintf(file, "%d %s\n", i, compression_map->map[i]);
        } else {
            // Handle NULL entries
            fprintf(file, "%d NULL\n", i);
        }
    }
    
    fclose(file);

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