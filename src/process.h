#ifndef PROCESS_H
#define PROCESS_H

typedef struct {
    int min_price;
    double avg_price;
    double std_dev;
    double min_price_per_unit_area;
} ResultStats;

ResultStats* compute_result(int* resale_prices, int* areas, int num_input_values);

# endif