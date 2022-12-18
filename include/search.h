#pragma once

#include <stdint.h>

int linear_search(const uint64_t *arr, int n, uint64_t key) ;
int linear_search_bitmask(const uint64_t *arr, int n, uint64_t key,uint64_t bit_mask) ;
int linear_search_avx (const uint64_t *arr, int n, uint64_t key) ;
int linear_search_avx_16(const uint64_t *arr, int n, uint64_t key);
int linear_search_avx_ur(const uint64_t *arr, int n, uint64_t key);
void set_ymm();
void clr_ymm();