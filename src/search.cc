#include "search.h"
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

int linear_search(const uint64_t *arr, int n, uint64_t key)
{
    intptr_t i = 0;
    while (i < n)
    {
        if (arr[i] == key)
            break;
        ++i;
    }
    return i;
}

#define SHUF(i0, i1, i2, i3) (i0 + i1 * 4 + i2 * 16 + i3 * 64)

void print_256(__m256i key)
{
    uint64_t *p = (uint64_t *)&key;
    for (int i = 0; i < 4; i++)
    {
        printf("%lx  ", p[i]);
    }
    printf("\n");
}

int linear_search_avx(const uint64_t *arr, int n, uint64_t key)
{
    const __m256i keys = _mm256_set1_epi64x(key);
    const auto rounded = 8 * (n / 8);
    for (size_t i = 0; i < rounded; i += 8)
    {

        const __m256i vec1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&arr[i]));
        const __m256i vec2 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&arr[i + 4]));

        const __m256i cmp1 = _mm256_cmpeq_epi64(vec1, keys);
        const __m256i cmp2 = _mm256_cmpeq_epi64(vec2, keys);

        const uint32_t mask1 = _mm256_movemask_epi8(cmp1);
        const uint32_t mask2 = _mm256_movemask_epi8(cmp2);

        if (mask1 != 0)
        {
            return i + __builtin_ctz(mask1) / 8;
        }
        else if (mask2 != 0)
        {
            return i + 4 + __builtin_ctz(mask2) / 8;
        }
    }

    for (size_t i = rounded; i < n; i++)
    {
        if (arr[i] == key)
        {
            return i;
        }
    }

    return -1;
}

/// @brief 增大avx一次操作的batch大小到16（默认一次处理8个数据）
/// @param arr
/// @param n
/// @param key
/// @return
int linear_search_avx_16(const uint64_t *arr, int n, uint64_t key)
{
    const __m256i keys = _mm256_set1_epi64x(key);
    const auto rounded = 16 * (n / 16);
    __m256i cnt = _mm256_setzero_si256();
    for (size_t i = 0; i < rounded; i += 16)
    {

        const __m256i vec1 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&arr[i]));
        const __m256i vec2 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&arr[i + 4]));
        const __m256i vec3 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&arr[i + 8]));
        const __m256i vec4 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&arr[i + 12]));

        const __m256i cmp1 = _mm256_cmpeq_epi64(vec1, keys);
        const __m256i cmp2 = _mm256_cmpeq_epi64(vec2, keys);
        const __m256i cmp3 = _mm256_cmpeq_epi64(vec3, keys);
        const __m256i cmp4 = _mm256_cmpeq_epi64(vec4, keys);

        const uint32_t mask1 = _mm256_movemask_epi8(cmp1);
        const uint32_t mask2 = _mm256_movemask_epi8(cmp2);
        const uint32_t mask3 = _mm256_movemask_epi8(cmp3);
        const uint32_t mask4 = _mm256_movemask_epi8(cmp4);
        if (mask1 != 0)
        {
            return i + __builtin_ctz(mask1) / 8;
        }
        else if (mask2 != 0)
        {
            return i + 4 + __builtin_ctz(mask2) / 8;
        }
        else if (mask3 != 0)
        {
            return i + 8 + __builtin_ctz(mask3) / 8;
        }
        else if (mask4 != 0)
        {
            return i + 12 + __builtin_ctz(mask4) / 8;
        }
    }

    for (size_t i = rounded; i < n; i++)
    {
        if (arr[i] == key)
        {
            return i;
        }
    }

    return -1;
}

/// @brief unrolled edtion of avx 
///         对小于256以下的部分进行直接展开
int linear_search_avx_ur(const uint64_t *arr, int n, uint64_t key)
{
    __m256i vkey = _mm256_set1_epi64x(key);
    __m256i cnt = _mm256_setzero_si256();
    intptr_t i = 0;
    #define STEP \
    if (i < 256) {\
        uint32_t mask1 = _mm256_movemask_epi8(_mm256_cmpeq_epi64(vkey, _mm256_loadu_si256((__m256i *)&arr[i+0]))); \
        uint32_t mask2 = _mm256_movemask_epi8(_mm256_cmpeq_epi64(vkey, _mm256_loadu_si256((__m256i *)&arr[i+4]))); \
        if (mask1 != 0) return i + __builtin_ctz(mask1) / 8;\
        if (mask2 != 0) return i + 4 + __builtin_ctz(mask2) / 8;\
    } i += 8;
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    STEP STEP STEP STEP STEP STEP STEP STEP
    // STEP STEP STEP STEP STEP STEP STEP STEP
    // STEP STEP STEP STEP STEP STEP STEP STEP
    // STEP STEP STEP STEP STEP STEP STEP STEP
    // STEP STEP STEP STEP STEP STEP STEP STEP
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

/// @brief 用来防止avx休眠
void set_ymm(){
    int data1 = 0;    
    int data2 = 0xffffffff;
    asm volatile("vpbroadcastd %%ebx,%%ymm0\n\t"
        "vpmovmskb %%ymm0,%%eax"         
        :"+a"(data1)
        :"b"(data2)
        );    
    // printf("data1:%x\n",data1);
}

void clr_ymm(){
    int data1 = 0;    
    int data2 = 0;
    asm("vpbroadcastd %%ebx,%%ymm0\n\t"
        "vpmovmskb %%ymm0,%%eax"         
        :"+a"(data1)
        :"b"(data2)
        ); 
    printf("data1:%x\n",data1);
}
