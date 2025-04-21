#include "functions.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

// Const Variable declarations


int record_size_array[11] = {
    YEAR_SIZE,
    MONTH_SIZE,
    TOWN_SIZE,
    FLAT_TYPE_SIZE,
    BLOCK_ATTR_SIZE,
    STREET_NAME_SIZE,
    STOREY_RANGE_SIZE,
    FLOOR_AREA_SQM_SIZE,
    FLAT_MODEL_SIZE,
    LEASE_COMMENCE_DATE_SIZE,
    RESALE_PRICE_SIZE
};



int is_int_column[11] = {1,1,0,0,0 ,0,0,1,0,1, 1};

char* column_names[] = {"year","month","town","flat_type","block","street_name","storey_range","floor_area_sqm","flat_model","lease_commence_date","resale_price"};


int storeColumnOrientedData(char *inputCsvFile, int total_num_records, ColumnMetaData* columnsMetaData) {
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
    
    printf("CSV headers:\n%s\n", headerLine);

    
    
    int count = 0;
    char line[1024];
    char *fields[10];
    Buffer* buffer = (Buffer*)malloc(sizeof(Buffer));
    
    if (buffer == NULL) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }

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
            strncpy(buffer->town[count], fields[1], TOWN_SIZE);
            strncpy(buffer->flat_type[count], fields[2], FLAT_TYPE_SIZE);
            strncpy(buffer->block[count], fields[3], BLOCK_ATTR_SIZE);
            strncpy(buffer->street_name[count], fields[4], STREET_NAME_SIZE);
            strncpy(buffer->storey_range[count], fields[5], STOREY_RANGE_SIZE);

            buffer->floor_area_sqm[count] = atoi(fields[6]);
            strncpy(buffer->flat_model[count], fields[7], FLAT_MODEL_SIZE);
            buffer->lease_commence_date[count] = atoi(fields[8]);
            buffer->resale_price[count] = atoi(fields[9]);

            count ++;
            
        }
    }

    save_buffer_to_binary_files(buffer, count);

   

    //ColumnMetaData columnsMetaData[11];
    for (int i=0; i<11; i++){
        create_metadata(buffer, &columnsMetaData[i], total_num_records, i);
    }

    // print min max for each partition

    for (int i=0; i<11; i++){
        printf("column: %s; total_records: %d, num_partitions %d\n", columnsMetaData[i].column_name, columnsMetaData[i].num_records_total, columnsMetaData[i].num_partitions);
        if (is_int_column[i]){
            for (int j =0; j< columnsMetaData[i].num_partitions; j++){
                printf("partition %d: max = %d, min = %d;\n", j, columnsMetaData[i].max_values[j], columnsMetaData[i].min_values[j]);
            }
        }
    }




    

    return count;
}


void create_metadata(Buffer *buffer, ColumnMetaData *columnMetaData, int num_records, int column_num){
    columnMetaData->num_records_total = num_records;
    columnMetaData->record_size = record_size_array[column_num];
    columnMetaData->num_records_per_partition = RECORDS_PER_PARTITION;
    columnMetaData->num_partitions = (num_records+RECORDS_PER_PARTITION-1)/RECORDS_PER_PARTITION;
    columnMetaData->column_name = column_names[column_num];

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
        if (system("if not exist .\\database mkdir .\\database") != 0) {
            printf("Failed to create database directory\n");
        }
    #else
        // Unix/Linux/macOS
        if (system("mkdir -p ./database") != 0) {
            printf("Failed to create database directory\n");
        }
    #endif

    // Save year data
    fp = fopen("./database/year.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->year, YEAR_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save month data
    fp = fopen("./database/month.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->month, MONTH_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save town data
    fp = fopen("./database/town.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->town, TOWN_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save flat_type data
    fp = fopen("./database/flat_type.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->flat_type, FLAT_TYPE_SIZE * 20, num_records, fp);
        fclose(fp);
    }
    
    // Save block data
    fp = fopen("./database/block.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->block, BLOCK_ATTR_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save street_name data
    fp = fopen("./database/street_name.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->street_name, STREET_NAME_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save storey_range data
    fp = fopen("./database/storey_range.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->storey_range, STOREY_RANGE_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save floor_area_sqm data
    fp = fopen("./database/floor_area_sqm.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->floor_area_sqm, FLOOR_AREA_SQM_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save flat_model data
    fp = fopen("./database/flat_model.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->flat_model, FLAT_MODEL_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save lease_commence_date data
    fp = fopen("./database/lease_commence_date.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->lease_commence_date, LEASE_COMMENCE_DATE_SIZE, num_records, fp);
        fclose(fp);
    }
    
    // Save resale_price data
    fp = fopen("./database/resale_price.bin", "wb");
    if (fp != NULL) {
        fwrite(buffer->resale_price, RESALE_PRICE_SIZE, num_records, fp);
        fclose(fp);
    }
    
    printf("All buffer data saved to binary files successfully.\n");
}




int *filter_year(int (*search_lines)[2], int num_rows, int filter_val, int record_size, int partition_size) {
    FILE *file = fopen("./database/year.bin", "r");


    // Allocate memory for result array (worst case: all records match)
    int *result = (int *)malloc(sizeof(int) * num_rows);
    int result_count = 0;

    
    // Buffer for reading data
    void *buffer = malloc(partition_size);


    // Process each search range
    for (int i = 0; i < num_rows; i++) {
        int start_idx = search_lines[i][0];
        int end_idx = search_lines[i][1];


        
        // Calculate how many complete partitions to read
        int num_records = end_idx - start_idx + 1;
        int num_partitions = (num_records * record_size + partition_size - 1) / partition_size;
        
        // Process each partition
        


        for (int p = 0; p < num_partitions; p++) {
            // Calculate starting position for this partition
            long position = (start_idx + p * (partition_size / record_size)) * record_size;
            fseek(file, position, SEEK_SET);
            
            // Read a partition
            int bytes_to_read = partition_size;
            if (p == num_partitions - 1) {
                // Last partition might be smaller
                int remaining_records = num_records - p * (partition_size / record_size);
                bytes_to_read = remaining_records * record_size;
            }

            fread(buffer, 1, bytes_to_read, file);

            // Process each record in the partition
            int records_in_partition = bytes_to_read / record_size;
            for (int r = 0; r < records_in_partition; r++) {
                int current_value = *((int *)((char *)buffer + r * record_size));
                int current_idx = start_idx + p * (partition_size / record_size) + r;
                
                // If value matches filter, add to result
                if (current_value == filter_val) {
                    result[result_count++] = current_idx;
                }
            }
        }
    }
    
    // Add terminator
    result[result_count] = -1;
    
    // Free temporary buffer
    free(buffer);
    fclose(file);
    
    return result;
}


int* get_indexes_filter_int(char* column_name, int* input_idx, int num_input_idx, int record_size, int filter_val){
    int last_partition_num = -1;
    int partition_num = -1;
    int count_input_idx = 0;

    int *result = (int *)malloc(sizeof(int) * num_input_idx);
    int count_result = 0;

    
    while (count_input_idx < num_input_idx){


        int partition_num_required = input_idx[count_input_idx] / RECORDS_PER_PARTITION;
        // if need to load a new partition
        if (partition_num_required > last_partition_num){
            last_partition_num = partition_num_required;

            // load the partition (cost one I/O)
            printf("getting partition num %d\n", partition_num_required);
            int *arr = get_IO_partition_int(partition_num_required,  record_size, column_name);

            // index number relative to a partition (0-999)
            int local_index;


            while ((count_input_idx < num_input_idx) && (input_idx[count_input_idx]/RECORDS_PER_PARTITION == partition_num_required)){
                local_index = input_idx[count_input_idx] % RECORDS_PER_PARTITION;
                if (arr[local_index] == filter_val){
                    result[count_result] = input_idx[count_input_idx];
                    count_result ++;
                }
                count_input_idx ++;
            }
        }
        else {
            count_input_idx ++;
        }

        
    }
    result[count_result] = -1;
    return result;

}



int* get_IO_partition_int(int partition_num, int record_size, char* column_name){
    char filename[100];
    sprintf(filename, "./database/%s.bin", column_name);

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
    
    // if (items_read != RECORDS_PER_PARTITION) {
    //     printf("Warning: Only read %zu of %d items\n", items_read, RECORDS_PER_PARTITION);
    // }

    return res;
}