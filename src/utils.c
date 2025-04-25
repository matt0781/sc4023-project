#include "stdio.h"
#include "stdlib.h"
#include "utils.h"
#include "const.h"
#include <time.h>
#include "filters.h"

  
void initDynamicArrayInt(DynamicArrayInt *a, int initialSize) {
    a->array = (int*)malloc(initialSize * sizeof(int));
    a->size = initialSize;
}

void freeArray(DynamicArrayInt *a) {
    free(a->array);
    a->array = NULL;
    a->size = 0;
}


void initDynamicArrayString(DynamicArrayString *a, int initialSize) {
    a->array = malloc(initialSize * sizeof(char*));
    for (int i = 0; i < initialSize; i++) {
        a->array[i] = NULL; // Initialize all pointers to NULL
    }
    a->size = initialSize;
}

void freeArrayString(DynamicArrayString *a) {
    // Free each string
    for (int i = 0; i < a->size; i++) {
        if (a->array[i] != NULL) {
            free(a->array[i]);
        }
    }
    
    // Free the array itself
    free(a->array);
    a->array = NULL;
    a->size = 0;
}



void write_res_to_csv(ResultStats** results, char** matric_nums, int num_inputs) {
    

    // Allocate memory for arrays
    int* years = malloc(num_inputs * sizeof(int));
    int* months = malloc(num_inputs * sizeof(int));
    int* towns = malloc(num_inputs * sizeof(int));
    for (int i=0; i<num_inputs;i++){
        years[i] =  (matric_nums[i][7]-'0') <= 3 ? 2020 + (matric_nums[i][7]-'0') : 2010 + (matric_nums[i][7]-'0') ;
        months[i] = matric_nums[i][6] == '0' ? 10: (matric_nums[i][6]-'0'); 
        towns[i] = matric_nums[i][5]-'0';
    }


    // Check if database directory exists, create if it doesn't
    #ifdef _WIN32
        // Windows
        if (system("if not exist .\\..\ScanResults mkdir .\\database") != 0) {
            printf("Failed to create ScanResults directory\n");
        }
    #else
        // Unix/Linux/macOS
        if (system("mkdir -p ./../ScanResults") != 0) {
            printf("Failed to create ScanResults directory\n");
        }
    #endif


    for (int i=0; i<num_inputs;i++){

        // Define filename and open file
        char filename[100];
        sprintf(filename, "./../ScanResults/ScanResult_%s.csv", matric_nums[i]);
        FILE* file = fopen(filename, "w");
        if (file == NULL) { printf("Error opening file %s\n", filename); return; }

        // Write header
        fprintf(file, "Year,Month,town,Category,Value\n");

        // Write content
        for(int j=0; j<4;j++){
            switch (j){
                case 0: fprintf(file, "%d,%02d,%s,%s,%.2d\n", years[i], months[i], town_mapping[towns[i]], "Minimum Price", results[i]->min_price); continue;
                case 1: fprintf(file, "%d,%02d,%s,%s,%.2f\n", years[i], months[i], town_mapping[towns[i]], "Average Price", results[i]->avg_price); continue;
                case 2: fprintf(file, "%d,%02d,%s,%s,%.2f\n", years[i], months[i], town_mapping[towns[i]], "Standard Deviation of Price", results[i]->std_dev); continue;
                case 3: fprintf(file, "%d,%02d,%s,%s,%.2f\n", years[i], months[i], town_mapping[towns[i]], "Minimum Price per Square Meter", results[i]->min_price_per_unit_area); continue;
            }
        }  
        fclose(file);      
    }
    
    
    printf("Results are successfully written into ScanResults directory.\n");
}


void calc_time_taken(char** inputs, int num_inputs, int total_num_records, ColumnMetaData* columnMetaData){
    // Allocate a 3D array with shape (num_trials (100), num_inputs (3), num_metrics (4))
    int num_trials = 100;
    double*** times = (double***)malloc(num_trials * sizeof(double**));
    for (int i = 0; i < 100; i++) {
        times[i] = (double**)malloc(num_inputs * sizeof(double*));
        for (int j = 0; j < num_inputs; j++) {
            times[i][j] = (double*)calloc(4, sizeof(double));
        }
    }   

    int* resale_prices;
    int* areas;

    for (int trial=0; trial < num_trials; trial++){
        for (int i=0; i<num_inputs; i++){

            for (int j = 0; j < 4; j++) {  // Changed inner loop variable from i to j
                Optimizer currentScanType = (Optimizer)j;
                clock_t start, end;
                double cpu_time_used;
                
                // Start timer
                start = clock();
                
                // Process based on scan type
                DynamicArrayInt* output_lines_;  // Declared only once outside the switch
                
                switch (currentScanType) {
                    case NORMAL:
                        output_lines_ = filter_scan(NORMAL, inputs[i], total_num_records, columnMetaData);
                        resale_prices = get_values_column("resale_price", output_lines_->array, output_lines_->size);
                        areas = get_values_column("floor_area_sqm", output_lines_->array, output_lines_->size);
                        compute_result(NORMAL, resale_prices, areas, output_lines_->size);
                        break;
                    case ZONE_MAP:
                        output_lines_ = filter_scan(ZONE_MAP, inputs[i], total_num_records, columnMetaData);
                        resale_prices = get_values_column("resale_price", output_lines_->array, output_lines_->size);
                        areas = get_values_column("floor_area_sqm", output_lines_->array, output_lines_->size);
                        compute_result(ZONE_MAP, resale_prices, areas, output_lines_->size);
                        break;
                    case SHARED_SCAN:
                        output_lines_ = filter_scan(SHARED_SCAN, inputs[i], total_num_records, columnMetaData);
                        resale_prices = get_values_column("resale_price", output_lines_->array, output_lines_->size);
                        areas = get_values_column("floor_area_sqm", output_lines_->array, output_lines_->size);
                        compute_result(SHARED_SCAN, resale_prices, areas, output_lines_->size);
                        break;
                    case ZONE_MAP_SHARED_SCAN:
                        output_lines_ = filter_scan(ZONE_MAP_SHARED_SCAN, inputs[i], total_num_records, columnMetaData);
                        resale_prices = get_values_column("resale_price", output_lines_->array, output_lines_->size);
                        areas = get_values_column("floor_area_sqm", output_lines_->array, output_lines_->size);
                        compute_result(ZONE_MAP_SHARED_SCAN, resale_prices, areas, output_lines_->size);
                        break;
                }
                
                // End timer
                end = clock();
                times[trial][i][j] = ((double) (end - start)) / CLOCKS_PER_SEC;
                
            }
        }
    }

        printf("\n");
        for(int i=0; i<num_inputs;i++){
            printf("Average  time taken %s from %d trials\n", inputs[i], num_trials);
            for (int j = 0; j < 4; j++){
                double avg = 0.0;
                for (int trial=0; trial < num_trials; trial++){
                    avg += times[trial][i][j];
                }
                avg /= num_inputs;
                
                printf("%-30s: %f seconds\n", 
                    j==0 ? "Normal Scan" : 
                    j==1 ? "Zone Map" : 
                    j==2 ? "Shared Scan" : 
                    j==3 ? "Zone Map + Shared Scan" : 
                    "Undefined scan method", 
                    avg);
            }
            printf("\n");
        }

    for (int i = 0; i < num_inputs; i++) {
        free(times[i]);
    }
    free(times);
}