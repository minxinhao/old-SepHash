#pragma once

#include <cstring>
#include <cstdio>
#include <vector>
#include <cmath>
#include <climits>
#include <immintrin.h>
#include <cassert>
#include <algorithm>
#include <mutex>
#include <avxintrin.h>

int linear_search(const uint64_t *arr, int n, uint64_t key) ;
int linear_search_avx (const uint64_t *arr, int n, uint64_t key) ;
int linear_search_avx_16(const uint64_t *arr, int n, uint64_t key);

/// @brief unrolled edtion of avx 
///         对小于1024以下的部分进行直接展开
template<intptr_t MAXN> int linear_search_avx_ur(const uint64_t *arr, int n, uint64_t key)
{
    __m256i vkey = _mm256_set1_epi64x(key);
    __m256i cnt = _mm256_setzero_si256();
    intptr_t i = 0;
    #define STEP \
    if (i < MAXN) {\
        uint32_t mask1 = _mm256_movemask_epi8(_mm256_cmpeq_epi64(vkey, _mm256_loadu_si256((__m256i *)&arr[i+0]))); \
        uint32_t mask2 = _mm256_movemask_epi8(_mm256_cmpeq_epi64(vkey, _mm256_loadu_si256((__m256i *)&arr[i+4]))); \
        if (mask1 != 0) return i + __builtin_ctz(mask1) / 8;\
        if (mask2 != 0) return i + 4 + __builtin_ctz(mask2) / 8;\
    } i += 8;
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    #undef STEP
    for (; i < n; i++)
    {
        if (arr[i] == key)
        {
            return i;
        }
    }

    return -1;
}