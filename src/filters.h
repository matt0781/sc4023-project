#ifndef FILTERS_H
#define FILTERS_H

#include "utils.h"
#include "load_db.h"


DynamicArrayInt* filter_scan(Optimizer scan_type, char* matric_num, int total_num_records, ColumnMetaData* columnMetaData);
DynamicArrayInt* get_lines_column(char* column_name, int* input_lines, int num_input_lines, int filter_val);
DynamicArrayInt* get_lines_column_zone_map(char* column_name, int* input_lines, int num_input_lines, int filter_val, int* valid_blocks, int num_valid_blocks);
void save_compression_map(CompressionMap* compression_map, char* filename);
int get_compression_key(CompressionMap* compression_map, char* compression_val);

int* get_values_column(char* column_name, int* input_lines, int num_input_lines);

#endif  