#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "const.h"
#include "process.h"
#include "utils.h"



ResultStats* compute_result(int* resale_prices, int* areas, int num_input_values) {
    if (num_input_values <= 0 || resale_prices == NULL || areas == NULL) {
        return NULL;
    }
    
    // Allocate memory for the results
    ResultStats* stats = (ResultStats*)malloc(sizeof(ResultStats));
    if (stats == NULL) {
        return NULL;
    }
    
    // Initialize results
    stats->min_price = resale_prices[0];
    double sum = 0.0;
    double min_price_per_area = (double)resale_prices[0] / areas[0];
    
    // First pass: calculate min, sum, and min price per unit area
    for (int i = 0; i < num_input_values; i++) {
        // Update minimum price
        if (resale_prices[i] < stats->min_price) {
            stats->min_price = resale_prices[i];
        }
        
        // Calculate price per unit area
        double price_per_area = (double)resale_prices[i] / areas[i];
        
        // Update minimum price per unit area
        if (price_per_area < min_price_per_area) {
            min_price_per_area = price_per_area;
        }
        
        // Add to sum for average calculation
        sum += resale_prices[i];
    }
    
    // Calculate average
    stats->avg_price = sum / num_input_values;
    stats->min_price_per_unit_area = min_price_per_area;
    
    // Second pass: calculate standard deviation
    double sum_squared_diff = 0.0;
    for (int i = 0; i < num_input_values; i++) {
        double diff = resale_prices[i] - stats->avg_price;
        sum_squared_diff += diff * diff;
    }
    
    stats->std_dev = sqrt(sum_squared_diff / num_input_values);
    
    return stats;
}


