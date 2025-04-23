#ifndef FILTERS_H
#define FILTERS_H

#include "utils.h"

DynamicArrayInt* filter_scan(ScanType scan_type, char* matric_num, int total_num_records);
DynamicArrayInt* get_lines_column(char* column_name, int* input_lines, int num_input_lines, int filter_val);
DynamicArrayInt* get_lines_filter_year(int* input_lines, int num_input_lines, int record_size, int filter_val);
DynamicArrayInt* get_lines_filter_month(int* input_lines, int num_input_lines, int record_size, int filter_val);
DynamicArrayInt* get_lines_filter_area(int* input_lines, int num_input_lines, int record_size, int filter_val);
DynamicArrayInt* get_lines_filter_town(int* input_lines, int num_input_lines, int record_size, int filter_val); // filter_val is the town integer mapping value

int* get_values_column(char* column_name, int* input_lines, int num_input_lines);

#endif  