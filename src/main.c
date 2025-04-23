#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "utils.h"
#include "functions.h"
#include "const.h"
#include "filters.h"
#include "process.h"



int main(void) {

    char *inputFile = "./../data/ResalePricesSingapore.csv";
    int total_num_records = 222833;

    ColumnMetaData* columnsMetaData = storeColumnOrientedData(inputFile,total_num_records);

    char *inputs[9] = {"U2223483D", "U2239382Q", "U2239521Q"};
    int num_inputs = 3;


    ResultStats** all_results = malloc(3 * sizeof(ResultStats*));

    for (int i=0; i<num_inputs; i++){

        printf("Processing result %d: %s\n", i, inputs[i]);

        // Get the lines by normal scanning, zone map, shared scan, zone map + shared scan
        DynamicArrayInt* output_lines_ = filter_scan(ZONE_MAP_SCAN, inputs[i], total_num_records, columnsMetaData);
        int* output_lines = output_lines_->array; // an array storing lines
        int output_size = output_lines_->size; // number of lines

        // Get the values of lines for computation
        int* resale_prices = get_values_column("resale_price", output_lines, output_size);
        int* areas = get_values_column("floor_area_sqm", output_lines, output_size);

        // Perform the computation
        all_results[i]= compute_result(resale_prices, areas, output_size);

        
    }

    write_res_to_csv(all_results, inputs, num_inputs);
    // Queries done //



    // Compare time taken for different scan methods
    calc_time_taken(inputs,  num_inputs, total_num_records, columnsMetaData);


    return 0;

}


