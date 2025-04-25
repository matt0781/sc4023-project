#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include "const.h"


// ----------------------------- Struct DECLARATIONS -----------------------------
typedef struct {
    int year[MAX_NUM_RECORDS];
    int month[MAX_NUM_RECORDS];
    int town[MAX_NUM_RECORDS];
    int flat_type[MAX_NUM_RECORDS];
    int block[MAX_NUM_RECORDS];
    int street_name[MAX_NUM_RECORDS];

    char storey_range[MAX_NUM_RECORDS];
    int floor_area_sqm[MAX_NUM_RECORDS];
    int flat_model[MAX_NUM_RECORDS];
    int lease_commence_date[MAX_NUM_RECORDS];
    int resale_price[MAX_NUM_RECORDS];
} Buffer;



typedef struct {
    char** map;
    int max_count;
    int count;
   
} CompressionMap;


typedef struct {
    // Partitions level attribute 
    // Note that we assume a partition can always hold 1000 records regardless of the datatype
    int *max_values; // stores max value of each partition
    int *min_values; // stores min value of each partition

    // Single value attribute
    char* column_name; // column name
    int record_size; // size of each record in bytes
    int num_partitions; // total number of partitions
    int num_records_total; // total number of records
    int num_records_per_partition; // number of records per partition


} ColumnMetaData;


// ----------------------------- FUNCTION DECLARATIONS -----------------------------
int write_to_file(int* column_data, char* column_name, int pageNum, int num_records);
void parseCSVLine(char* line, char* fields[], int numFields);
int* get_column_data_pointer(Buffer* buffer, int field_index);
ColumnMetaData* storeColumnOrientedData(char *inputCsvFile, int total_num_records);


void save_buffer_to_binary_files(Buffer* buffer, int num_records);
void create_metadata(Buffer *buffer, ColumnMetaData *columnMetaData, int num_records, int column_num);


int get_digit_by_town(const char* town_name);
int get_column_id_by_name(char* column_name);

int get_compression_key(CompressionMap* compression_map, char* compression_val);
void save_compression_map(CompressionMap* compression_map, char* filename);

#endif  // End of include guard