#pragma once
#include <stdint.h>
#include <stddef.h>

size_t hash_1(const void* _ptr, size_t _len);
size_t hash_2(const void* _ptr, size_t _len);
__uint128_t hash(const void *key, const int len, const uint32_t seed = 0) noexcept;


