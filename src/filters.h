#ifndef FILTERS_H
#define FILTERS_H

#include "utils.h"
#include "functions.h"


DynamicArrayInt* filter_scan(ScanType scan_type, char* matric_num, int total_num_records, ColumnMetaData* columnMetaData);
DynamicArrayInt* get_lines_column(char* column_name, int* input_lines, int num_input_lines, int filter_val);
DynamicArrayInt* get_lines_column_zone_map(char* column_name, int* input_lines, int num_input_lines, int filter_val, int* valid_blocks, int num_valid_blocks);

int* get_values_column(char* column_name, int* input_lines, int num_input_lines);

#endif  