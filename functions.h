#ifndef FUNCTIONS 
#define FUNCTIONS  

// CONSTANTS

#define MAX_NUM_RECORDS 300000
#define TOTAL_NUM_RECORDS 222833
#define RECORDS_PER_PARTITION 1000
#define MAX_NUM_PARTITON 300

// metadata - size of each attribute
#define YEAR_SIZE sizeof(int)
#define MONTH_SIZE sizeof(int)
#define TOWN_SIZE 30
#define FLAT_TYPE_SIZE 20
#define BLOCK_ATTR_SIZE 4
#define STREET_NAME_SIZE 30

#define STOREY_RANGE_SIZE 7
#define FLOOR_AREA_SQM_SIZE sizeof(int)
#define FLAT_MODEL_SIZE 30
#define LEASE_COMMENCE_DATE_SIZE sizeof(int)
#define RESALE_PRICE_SIZE sizeof(int)

// metadata - number of values of an attribute per block
#define NUM_YEAR_PER_BLOCK BLOCK_SIZE/YEAR_SIZE
#define NUM_MONTH_PER_BLOCK BLOCK_SIZE/MONTH_SIZE
#define NUM_TOWN_PER_BLOCK BLOCK_SIZE/TOWN_SIZE
#define NUM_FLAT_TYPE_PER_BLOCK BLOCK_SIZE/FLAT_TYPE_SIZE
#define NUM_BLOCK_PER_BLOCK BLOCK_SIZE/BLOCK_ATTR_SIZE
#define NUM_STREET_NAME_PER_BLOCK BLOCK_SIZE/STREET_NAME_SIZE

#define NUM_STOREY_RANGE_PER_BLOCK BLOCK_SIZE/STOREY_RANGE_SIZE
#define NUM_FLOOR_AREA_SQM_PER_BLOCK BLOCK_SIZE/FLOOR_AREA_SQM_SIZE
#define NUM_FLAT_MODEL_PER_BLOCK BLOCK_SIZE/FLAT_MODEL_SIZE
#define NUM_LEASE_COMMENCE_DATE_PER_BLOCK BLOCK_SIZE/LEASE_COMMENCE_DATE_SIZE
#define NUM_RESALE_PRICE_PER_BLOCK BLOCK_SIZE/RESALE_PRICE_SIZE



// ----------------------------- Struct DECLARATIONS -----------------------------
typedef struct {
    int year[MAX_NUM_RECORDS];
    int month[MAX_NUM_RECORDS];
    char town[MAX_NUM_RECORDS][30];
    char flat_type[MAX_NUM_RECORDS][20];
    char block[MAX_NUM_RECORDS][4];
    char street_name[MAX_NUM_RECORDS][30];

    char storey_range[MAX_NUM_RECORDS][20];
    int floor_area_sqm[MAX_NUM_RECORDS];
    char flat_model[MAX_NUM_RECORDS][30];
    int lease_commence_date[MAX_NUM_RECORDS];
    int resale_price[MAX_NUM_RECORDS];
} Buffer;

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
int storeColumnOrientedData(char *inputCsvFile, int total_num_records, ColumnMetaData* columnsMetaData);

int *filter_year(int (*search_lines)[2], int num_rows, int filter_val, int record_size, int partition_size);
void save_buffer_to_binary_files(Buffer* buffer, int num_records);
void create_metadata(Buffer *buffer, ColumnMetaData *columnMetaData, int num_records, int column_num);

int* get_IO_partition_int(int partition_num, int record_size, char* column_name);
int* get_indexes_filter_int(char* column_name, int* input_idx, int num_input_idx, int record_size, int filter_val);
#endif  // End of include guard