
#ifndef DISK_H
#define DISK_H

int* get_IO_partition_int(int partition_num, int record_size, char* column_name);
char** get_IO_partition_char(int partition_num, int record_size, char* column_name);

#endif