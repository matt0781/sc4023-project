/* const.c or another appropriate .c file */
#include "const.h"

/* Define the arrays (note the const qualifier) */
const int record_size_mapping[11] = {
    YEAR_SIZE,
    MONTH_SIZE,
    TOWN_SIZE,
    FLAT_TYPE_SIZE,
    BLOCK_ATTR_SIZE,
    STREET_NAME_SIZE,
    STOREY_RANGE_SIZE,
    FLOOR_AREA_SQM_SIZE,
    FLAT_MODEL_SIZE,
    LEASE_COMMENCE_DATE_SIZE,
    RESALE_PRICE_SIZE
};

const int is_int_column[11] = {1,1,0,0,0,0,0,1,0,1,1};

const char* column_name_mapping[11] = {
    "year",
    "month",
    "town",
    "flat_type",
    "block",
    "street_name",
    "storey_range",
    "floor_area_sqm",
    "flat_model",
    "lease_commence_date",
    "resale_price"
};

const char* town_mapping[26] = {
    "BEDOK",          // 0
    "BUKIT PANJANG",  // 1
    "CLEMENTI",       // 2
    "CHOA CHU KANG",  // 3
    "HOUGANG",        // 4
    "JURONG WEST",    // 5
    "PASIR RIS",      // 6
    "TAMPINES",       // 7
    "WOODLANDS",      // 8
    "YISHUN",         // 9
    "ANG MO KIO",     // 10
    "BISHAN",         // 11
    "BUKIT BATOK",    // 12
    "BUKIT MERAH",    // 13
    "BUKIT TIMAH",    // 14
    "CENTRAL AREA",   // 15
    "GEYLANG",        // 16
    "JURONG EAST",    // 17
    "KALLANG/WHAMPOA",// 18
    "MARINE PARADE",  // 19
    "PUNGGOL",        // 20
    "QUEENSTOWN",     // 21
    "SEMBAWANG",      // 22
    "SENGKANG",       // 23
    "SERANGOON",      // 24
    "TOA PAYOH"       // 25
};
