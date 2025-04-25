#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "const.h"
#include "compute.h"
#include "utils.h"


ResultStats* compute_result(Optimizer optimizer, int* resale_prices, int* areas, int num_lines) {
    if (num_lines <= 0 || resale_prices == NULL || areas == NULL) {
        return NULL;
    }
        
    // Initialize results
    int min_price = resale_prices[0];
    double avg_price = 0.0;
    double sd_price = 0.0;
    double min_price_per_area = (double)resale_prices[0] / areas[0];

    if ((optimizer == SHARED_SCAN) || (optimizer == ZONE_MAP_SHARED_SCAN)){
        for (int i = 0; i < num_lines; i++) {
            min_price = resale_prices[i]< min_price ? resale_prices[i]: min_price;
            min_price_per_area = (double)resale_prices[i] / areas[i] < min_price_per_area ? (double)resale_prices[i] / areas[i] : min_price_per_area;
            avg_price += (double)resale_prices[i];
        }
        avg_price /= num_lines;
        for (int i = 0; i < num_lines; i++) {
            sd_price += (resale_prices[i] - avg_price)*(resale_prices[i] - avg_price)/num_lines;
        }
        sd_price = sqrt(sd_price);
    }
    else{
        for (int i = 0; i < num_lines; i++) {
            min_price = resale_prices[i]< min_price ? resale_prices[i]: min_price;
        }
        for (int i = 0; i < num_lines; i++) {
            min_price_per_area = (double)resale_prices[i] / areas[i] < min_price_per_area ? (double)resale_prices[i] / areas[i] : min_price_per_area;
        }
        for (int i = 0; i < num_lines; i++) {
            avg_price += (double)resale_prices[i];
        }
        avg_price /= num_lines;
        for (int i = 0; i < num_lines; i++) {
            sd_price += (resale_prices[i] - avg_price)*(resale_prices[i] - avg_price)/num_lines;
        }
        sd_price = sqrt(sd_price);

    }
    
    ResultStats* stats = (ResultStats*)malloc(sizeof(ResultStats));
    if (stats == NULL) {
        return NULL;
    }  
    stats->min_price = min_price;
    stats->avg_price = avg_price;
    stats->std_dev = sd_price;
    stats->min_price_per_unit_area = min_price_per_area;
    
    return stats;
}






