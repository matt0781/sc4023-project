#ifndef COMPUTE_H
#define COMPUTE_H

#include "const.h"
typedef struct {
    int min_price;
    double avg_price;
    double std_dev;
    double min_price_per_unit_area;
} ResultStats;

ResultStats* compute_result(Optimizer optmizer, int* resale_prices, int* areas, int num_input_values);

# endif