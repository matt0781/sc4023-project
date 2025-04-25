#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "utils.h"
#include "load_db.h"
#include "const.h"
#include "filters.h"
#include "compute.h"
#include "global.h"


int main(void) {

    char *inputFile = "./../data/ResalePricesSingapore.csv";
    int total_num_records = 222833;

    ColumnMetaData* columnsMetaData = storeColumnOrientedData(inputFile,total_num_records);

    // ====== MODIFY HERE to test with different Matric Number ==========
    // === 1. Add your matric number into the set `inputs[9]`
    // === 2. Update num_inputs to total number of Matric number in the set =====
    char *inputs[9] = {"U2223483D", "U2123083J", "U2122248G"};
    int num_inputs = 3;
    // ====================================================================

    ResultStats** all_results = malloc(3 * sizeof(ResultStats*));
    Optimizer optimizers[4] = {NORMAL, ZONE_MAP, SHARED_SCAN, ZONE_MAP_SHARED_SCAN};
    initGlobalIOTracer();


    for (int i=0; i<num_inputs; i++){

        for (int j=0; j<4;j++){

            // Retrieve the lines (indexes) of resale price
            DynamicArrayInt* output_lines_ = filter_scan(optimizers[j], inputs[i], total_num_records, columnsMetaData);

            // Retrieve the values resale_price 
            int* resale_prices = get_values_column("resale_price", output_lines_->array, output_lines_->size);
            int* areas = get_values_column("floor_area_sqm", output_lines_->array, output_lines_->size);

            // Perform the computation
            all_results[i]= compute_result(optimizers[j], resale_prices, areas, output_lines_->size);

            // Print number of I/O
            if (i==0){
                printf("Number of I/Os for %s:\n", inputs[i]);
                print_globalIO(j);
                resetGlobalIOTracer();
            }
            
        }

        // disable globalIO tracer after the first matric number
        disableGlobalIOTracer();
        
    }

    write_res_to_csv(all_results, inputs, num_inputs);
    // Queries done //



    // Compare time taken for different scan methods
    calc_time_taken(inputs,  num_inputs, total_num_records, columnsMetaData);


    return 0;

}


