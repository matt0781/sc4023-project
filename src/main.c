#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "utils.h"
#include "functions.h"
#include "const.h"
#include "filters.h"
#include "compute.h"



int main(void) {

    char *inputFile = "./../data/ResalePricesSingapore.csv";
    int total_num_records = 222833;

    ColumnMetaData* columnsMetaData = storeColumnOrientedData(inputFile,total_num_records);

    char *inputs[9] = {"U2223483D", "U2123083J", "U2122248G"};
    int num_inputs = 3;


    ResultStats** all_results = malloc(3 * sizeof(ResultStats*));
    Optimizer optimizer = ZONE_MAP_SHARED_SCAN; // can choose from NORMAL, ZONE_MAP, SHARED_SCAN, ZONE_MAP_SHARED_SCAN

    for (int i=0; i<num_inputs; i++){

        printf("Processing result %d: %s\n", i, inputs[i]);

        // Retrieve the lines (indexes) of resale price
        DynamicArrayInt* output_lines_ = filter_scan(optimizer, inputs[i], total_num_records, columnsMetaData);

        // Retrieve the values resale_price 
        int* resale_prices = get_values_column("resale_price", output_lines_->array, output_lines_->size);
        int* areas = get_values_column("floor_area_sqm", output_lines_->array, output_lines_->size);

        // Perform the computation
        all_results[i]= compute_result(optimizer, resale_prices, areas, output_lines_->size);
    }

    write_res_to_csv(all_results, inputs, num_inputs);
    // Queries done //



    // Compare time taken for different scan methods
    calc_time_taken(inputs,  num_inputs, total_num_records, columnsMetaData);


    return 0;

}


