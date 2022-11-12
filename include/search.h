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

#define SHUF(i0, i1, i2, i3) (i0 + i1*4 + i2*16 + i3*64)

// power of 2 at most x, undefined for x == 0
uint32_t bsr(uint32_t x) {
  return 31 - __builtin_clz(x);
}

static int binary_search_std (const int *arr, int n, int key) {
    return (int) (std::lower_bound(arr, arr + n, key) - arr);
}

static int binary_search_simple(const int *arr, int n, int key) {
  intptr_t left = -1;
  intptr_t right = n;
  while (right - left > 1) {
    intptr_t middle = (left + right) >> 1;
    if (arr[middle] < key)
      left = middle;
    else
      right = middle;
  }
  return (int) right;
}

template<typename KEY_TYPE>
static int binary_search_branchless(const KEY_TYPE *arr, int n, KEY_TYPE key) {
//static int binary_search_branchless(const int *arr, int n, int key) {
  intptr_t pos = -1;
  intptr_t logstep = bsr(n - 1);
  intptr_t step = intptr_t(1) << logstep;

  pos = (arr[pos + n - step] < key ? pos + n - step : pos);
  step >>= 1;

  while (step > 0) {
    pos = (arr[pos + step] < key ? pos + step : pos);
    step >>= 1;
  }
  pos += 1;

  return (int) (arr[pos] >= key ? pos : n);
}

/*static int binary_search_branchless(const int64_t *arr, int n, int64_t key) {
  intptr_t pos = -1;
  intptr_t logstep = bsr(n - 1);
  intptr_t step = intptr_t(1) << logstep;

  pos = (arr[pos + n - step] < key ? pos + n - step : pos);
  step >>= 1;

  while (step > 0) {
    pos = (arr[pos + step] < key ? pos + step : pos);
    step >>= 1;
  }
  pos += 1;

  return (int) (arr[pos] >= key ? pos : n);
}*/

static uint32_t interpolation_search( const int32_t* data, uint32_t n, int32_t key ) {
  uint32_t low = 0;
  uint32_t high = n-1;
  uint32_t mid;

  if ( key <= data[low] ) return low;

  uint32_t iters = 0;
  while ( data[high] > data[low] and
          data[high] > key and
          data[low] < key ) {
    iters += 1;
    if ( iters > 1 ) return binary_search_branchless( data + low, high-low, key );
    
    mid = low + (((long)key - (long)data[low]) * (double)(high - low) / ((long)data[high] - (long)data[low]));

    if ( data[mid] < key ) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  if ( key <= data[low] ) return low;
  if ( key <= data[high] ) return high;
  return high + 1;
}

static int linear_search(const int *arr, int n, int key) {
  intptr_t i = 0;
  while (i < n) {
    if (arr[i] == key)
      break;
    ++i;
  }
  return i;
}

// 有Bug还
static int linear_search_avx (const int *arr, int n, int key) {
  __m256i vkey = _mm256_set1_epi32(key);
  __m256i cnt = _mm256_setzero_si256();
  for (int i = 0; i < n; i += 16) {
    __m256i mask0 = _mm256_cmpgt_epi32(vkey, _mm256_loadu_si256((__m256i *)&arr[i+0]));
    __m256i mask1 = _mm256_cmpgt_epi32(vkey, _mm256_loadu_si256((__m256i *)&arr[i+8]));
    __m256i sum = _mm256_add_epi32(mask0, mask1);
    cnt = _mm256_sub_epi32(cnt, sum);
  }
  __m128i xcnt = _mm_add_epi32(_mm256_extracti128_si256(cnt, 1), _mm256_castsi256_si128(cnt));
  xcnt = _mm_add_epi32(xcnt, _mm_shuffle_epi32(xcnt, SHUF(2, 3, 0, 1)));
  xcnt = _mm_add_epi32(xcnt, _mm_shuffle_epi32(xcnt, SHUF(1, 0, 3, 2)));
  return _mm_cvtsi128_si32(xcnt);
}

static int linear_search_avx_8(const int *arr, int n, int key) {
    __m256i vkey = _mm256_set1_epi32(key);
    __m256i cnt = _mm256_setzero_si256();

    for (int i = 0; i < n; i += 8) {
        __m256i mask0 = _mm256_cmpgt_epi32(vkey, _mm256_loadu_si256((__m256i *)&arr[i+0]));
        cnt = _mm256_sub_epi32(cnt, mask0);
    }

    __m128i xcnt = _mm_add_epi32(_mm256_extracti128_si256(cnt, 1), _mm256_castsi256_si128(cnt));
    xcnt = _mm_add_epi32(xcnt, _mm_shuffle_epi32(xcnt, SHUF(2, 3, 0, 1)));
    xcnt = _mm_add_epi32(xcnt, _mm_shuffle_epi32(xcnt, SHUF(1, 0, 3, 2)));

    return _mm_cvtsi128_si32(xcnt);
}

static void print_256(__m256i key){
    int32_t *p = (int*)&key;
    for (int i = 0; i < 8; i++){
        printf("%d  ", p[i]);
    }
    printf("\n");
}
static int linear_search_avx(const int64_t *arr, int n, int64_t key) {
    __m256i vkey = _mm256_set1_epi64x(key);
    __m256i cnt = _mm256_setzero_si256();
    for (int i = 0; i < n; i += 8) {
      __m256i mask0 = _mm256_cmpgt_epi64(vkey, _mm256_loadu_si256((__m256i *)&arr[i+0]));
      __m256i mask1 = _mm256_cmpgt_epi64(vkey, _mm256_loadu_si256((__m256i *)&arr[i+4]));
      __m256i sum = _mm256_add_epi64(mask0, mask1);
      cnt = _mm256_sub_epi64(cnt, sum);
    }
    __m128i xcnt = _mm_add_epi64(_mm256_extracti128_si256(cnt, 1), _mm256_castsi256_si128(cnt));
    xcnt = _mm_add_epi64(xcnt, _mm_shuffle_epi32(xcnt, SHUF(2, 3, 0, 1)));
    return _mm_cvtsi128_si32(xcnt);
}


template<class T>
int linear_search_sse(const T *arr, const int n,T key)  {

    const __m128i keys = _mm_set1_epi32(key);
    const auto rounded = 8 * (n/8);

    for (size_t i=0; i < rounded; i += 8) {

        const __m128i vec1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr[i]));
        const __m128i vec2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr[i + 4]));

        const __m128i cmp1 = _mm_cmpeq_epi32(vec1, keys);
        const __m128i cmp2 = _mm_cmpeq_epi32(vec2, keys);

        const __m128i tmp  = _mm_packs_epi32(cmp1, cmp2);
        const uint32_t mask = _mm_movemask_epi8(tmp);

        if (mask != 0) {
            return i + __builtin_ctz(mask)/2;
        }
    }

    for (size_t i = rounded; i < n; i++) {
        if (arr[i] == key) {
            return i;
        }
    }

    return -1;
}


int binsearch(const int *data,int n,int key) {
    int a=0, b=n;
    while (a <= b) {
        const int c = (a + b)/2;

        if (data[c] == key) {
            return c;
        }

        if (key < data[c]) {
            b = c - 1;
        } else {
            a = c + 1;
        }
    }

    return -1;
}

int binary_search_branchless(const int *arr, int n, int key) {
  intptr_t pos = -1;
  intptr_t logstep = bsr(n - 1);
  intptr_t step = intptr_t(1) << logstep;

  pos = (arr[pos + n - step] < key ? pos + n - step : pos);
  step >>= 1;

  while (step > 0) {
    pos = (arr[pos + step] < key ? pos + step : pos);
    step >>= 1;
  }
  pos += 1;

  return (int) (arr[pos] >= key ? pos : n);
}

int binsearch_sse(const int *data,int n,int key){

    const __m128i keys = _mm_set1_epi32(key);
    __m128i v;

    int limit = n - 1;
    int a = 0;
    int b = limit;

    while (a <= b) {
        const int c = (a + b)/2;

        if (data[c] == key) {
            return c;
        }

        if (key < data[c]) {
            b = c - 1;

            if (b >= 4) {
                v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&data[b - 4]));
                v = _mm_cmpeq_epi32(v, keys);
                const uint16_t mask = _mm_movemask_epi8(v);
                if (mask) {
                    return b - 4 + __builtin_ctz(mask)/4;
                }
            }
        } else {
            a = c + 1;

            if (a + 4 < limit) {
                v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&data[a]));
                v = _mm_cmpeq_epi32(v, keys);
                const uint16_t mask = _mm_movemask_epi8(v);
                if (mask) {
                    return a + __builtin_ctz(mask)/4;
                }
            }
        }
    }

    return -1;
}

