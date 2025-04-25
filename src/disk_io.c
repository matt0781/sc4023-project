#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "const.h"
#include "load_db.h"
#include "global.h"


int* get_IO_int(int partition_num, int record_size, char* column_name){
    char filename[100];
    sprintf(filename, "./../database/%s.bin", column_name);

    FILE *file = fopen(filename, "rb");
    if (!file) {
        printf("Error: Cannot open file %s\n", filename);
        return NULL;
    }

    long startAddress = partition_num * RECORDS_PER_PARTITION * record_size;
    fseek(file, startAddress, SEEK_SET);

    int* res = (int*)malloc(RECORDS_PER_PARTITION*sizeof(int));


    size_t items_read = fread(res, sizeof(int), RECORDS_PER_PARTITION , file);

    fclose(file);
    

    addGlobalIO();

    return res;
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
