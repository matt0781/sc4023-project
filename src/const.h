#ifndef CONST_H
#define CONST_H


#define MAX_NUM_RECORDS 300000
#define TOTAL_NUM_RECORDS 222833
#define RECORDS_PER_PARTITION 1000
#define MAX_NUM_PARTITON 300

// metadata - size of each attribute
#define YEAR_SIZE sizeof(int)
#define MONTH_SIZE sizeof(int)
#define TOWN_SIZE sizeof(int)
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


/* Declare arrays as extern const since they'll be defined in a .c file */
extern const int record_size_mapping[11];
extern const int is_int_column[11];
extern const char* column_name_mapping[11];


/* 4 types of scans */
typedef enum {
    NORMAL_SCAN = 0,      // Normal scan
    ZONE_MAP_SCAN = 1,    // Zone map scan
    SHARED_SCAN = 2,      // Shared scan
    ZONE_MAP_SHARED_SCAN = 3  // Zone map + shared scan
} ScanType;



const char* town_mapping[26];
#endif /* CONST_H */