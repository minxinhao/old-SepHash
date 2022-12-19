#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <map>
#include <numeric>
#include <stdint.h>
#include <string>
#include <tuple>
#include <vector>
#include <string.h>
namespace sort
{

const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
const uint64_t kFNVPrime64 = 1099511628211;

inline uint64_t FNVHash64(uint64_t val)
{
    uint64_t hash = kFNVOffsetBasis64;

    for (int i = 0; i < 8; i++)
    {
        uint64_t octet = val & 0x00ff;
        val = val >> 8;

        hash = hash ^ octet;
        hash = hash * kFNVPrime64;
    }
    return hash;
}

struct Slot
{
    uint8_t fp : 8;
    uint8_t len : 4;
    uint8_t sign : 1; // 用来表示split delete信息
    uint8_t dep : 3;
    uint64_t offset : 48;
    operator uint64_t()
    {
        return *(uint64_t *)this;
    }
    Slot(uint64_t u)
    {
        *this = *(Slot *)(&u);
    }

    bool operator<(const Slot &a) const
    { // 必须加const
        return fp < a.fp;
    }

    void print()
    {
        printf("fp:%d\t", fp);
        printf("len:%d\t", len);
        printf("sign:%d\t", sign);
        printf("dep:%d\t", dep);
        printf("offset:%lu\t", offset);
        printf("size:%ld\n", sizeof(Slot));
    }
} __attribute__((aligned(8)));

inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
}

#ifdef __GNUC__
#define FORCE_INLINE __attribute__((always_inline)) inline
#else
#define FORCE_INLINE inline
#endif

static FORCE_INLINE uint32_t rotl32(uint32_t x, int8_t r)
{
    return (x << r) | (x >> (32 - r));
}

static FORCE_INLINE uint64_t rotl64(uint64_t x, int8_t r)
{
    return (x << r) | (x >> (64 - r));
}

#define ROTL32(x, y) rotl32(x, y)
#define ROTL64(x, y) rotl64(x, y)

#define BIG_CONSTANT(x) (x##LLU)

#define getblock(p, i) (p[i])

static FORCE_INLINE uint64_t fmix64(uint64_t k)
{
    k ^= k >> 33;
    k *= BIG_CONSTANT(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;

    return k;
}

// murmur3 hash
__uint128_t hash(const void *key, const int len, const uint32_t seed) noexcept
{
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 16;
    int i;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    uint64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
    uint64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);

    //----------
    // body

    const uint64_t *blocks = (const uint64_t *)(data);

    for (i = 0; i < nblocks; i++)
    {
        uint64_t k1 = getblock(blocks, i * 2 + 0);
        uint64_t k2 = getblock(blocks, i * 2 + 1);

        k1 *= c1;
        k1 = ROTL64(k1, 31);
        k1 *= c2;
        h1 ^= k1;

        h1 = ROTL64(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        k2 *= c2;
        k2 = ROTL64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

        h2 = ROTL64(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
    }

    //----------
    // tail

    const uint8_t *tail = (const uint8_t *)(data + nblocks * 16);

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15)
    {
    case 15:
        k2 ^= (uint64_t)(tail[14]) << 48;
    case 14:
        k2 ^= (uint64_t)(tail[13]) << 40;
    case 13:
        k2 ^= (uint64_t)(tail[12]) << 32;
    case 12:
        k2 ^= (uint64_t)(tail[11]) << 24;
    case 11:
        k2 ^= (uint64_t)(tail[10]) << 16;
    case 10:
        k2 ^= (uint64_t)(tail[9]) << 8;
    case 9:
        k2 ^= (uint64_t)(tail[8]) << 0;
        k2 *= c2;
        k2 = ROTL64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

    case 8:
        k1 ^= (uint64_t)(tail[7]) << 56;
    case 7:
        k1 ^= (uint64_t)(tail[6]) << 48;
    case 6:
        k1 ^= (uint64_t)(tail[5]) << 40;
    case 5:
        k1 ^= (uint64_t)(tail[4]) << 32;
    case 4:
        k1 ^= (uint64_t)(tail[3]) << 24;
    case 3:
        k1 ^= (uint64_t)(tail[2]) << 16;
    case 2:
        k1 ^= (uint64_t)(tail[1]) << 8;
    case 1:
        k1 ^= (uint64_t)(tail[0]) << 0;
        k1 *= c1;
        k1 = ROTL64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;
    // h1 += h1 == h2 ? 0x125e591ull : 0ull;

    return (__uint128_t)h1 << 64 | h2;
}

uint64_t find_free_slot(uint64_t pos, Slot *new_seg, uint64_t new_len)
{
    // printf("pos:%lu\n",pos);
    uint64_t bucket_size = 256;
    for (uint64_t tmp = pos; tmp < pos + 2 * bucket_size && tmp < new_len; tmp++)
    {
        // printf("pos:%lu new_seg[%lu]:%lu\n",tmp,tmp,(uint64_t)new_seg[tmp]);
        if (new_seg[tmp] == 0)
            return tmp;
    }
    for (uint64_t tmp = 0; tmp < bucket_size; tmp++)
    {
        // printf("pos:%lu new_seg[%lu]:%lu\n",pos - tmp,pos - tmp,(uint64_t)new_seg[pos - tmp]);
        if (pos >= tmp && new_seg[pos - tmp] == 0)
            return tmp;
    }
    // return -1;
    printf("err\n");
    return 0;
};

void insert_sort(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg)
{
    for (uint64_t i = 0; i < len; i++)
    {
        // printf("data-%lu\n",i);
        uint64_t pos = (data[i].fp * (len + old_seg_len)) / UINT8_MAX;
        pos = find_free_slot(pos, new_seg, len + old_seg_len);
        new_seg[pos] = data[i];
    }

    for (uint64_t i = 0; i < old_seg_len; i++)
    {
        // printf("old_seg-%lu\n",i);
        uint64_t pos = (old_seg[i].fp * (len + old_seg_len)) / UINT8_MAX;
        pos = find_free_slot(pos, new_seg, len + old_seg_len);
        new_seg[pos] = old_seg[i];
    }
}

void test_insert_sort()
{
    for (uint64_t sort_size = 128; sort_size <= 128; sort_size *= 2)
    {
        Slot *data = (Slot *)malloc(sizeof(Slot) * sort_size);
        for (uint64_t old_seg_size = 4; old_seg_size <= 128; old_seg_size++)
        {
            std::map<int, int> cnt;
            for (uint64_t i = 0; i < sort_size; i++)
            {
                uint64_t tmp = FNVHash64(i);
                uint64_t pattern = hash(&tmp, sizeof(uint64_t), 0);
                data[i].fp = fp(pattern);
                cnt[data[i].fp] = 1;
            }
            Slot *old_seg = (Slot *)malloc(sizeof(Slot) * sort_size * old_seg_size);
            for (uint64_t i = sort_size; i < sort_size * (old_seg_size + 1); i++)
            {
                uint64_t tmp = FNVHash64(i);
                uint64_t pattern = hash(&tmp, sizeof(uint64_t), 0);
                old_seg[i - sort_size].fp = fp(pattern);
                cnt[old_seg[i - sort_size].fp] = 1;
            }
            printf("sort_size:%lu old_seg_size:%lu - Avg Occur:%lf\n", sort_size, old_seg_size,
                   (1.0 * sort_size * (old_seg_size + 1)) / cnt.size());
            Slot *new_seg = (Slot *)malloc(sort_size * (old_seg_size + 1) * sizeof(Slot));
            memset(new_seg,0,sort_size * (old_seg_size + 1) * sizeof(Slot));
            auto start = std::chrono::steady_clock::now();
            insert_sort(data, sort_size, old_seg, sort_size * old_seg_size, new_seg);
            auto end = std::chrono::steady_clock::now();
            double duration = std::chrono::duration<double, std::micro>(end - start).count();
            printf("Load duration:%.2lfus\n", duration);
            free(new_seg);
            free(old_seg);
        }
        free(data);
    }
}

void test_stdsort()
{
    Slot *data = (Slot *)malloc(sizeof(Slot) * 1024);
    for (uint64_t sort_size = 128; sort_size <= 1024; sort_size *= 2)
    {
        std::map<int, int> cnt;
        for (uint64_t i = 0; i < sort_size; i++)
        {
            uint64_t tmp = FNVHash64(i);
            uint64_t pattern = hash(&tmp, sizeof(uint64_t), 0);
            data[i].fp = fp(pattern);
            if (cnt.count(data[i].fp) == 0)
            {
                cnt[data[i].fp] = 0;
            }
            else
            {
                cnt[data[i].fp]++;
            }
        }
        printf("sort_size:%lu - Avg Occur:%lf\n", sort_size, (1.0 * sort_size) / cnt.size());
        auto start = std::chrono::steady_clock::now();
        std::sort(data, data + sort_size);
        auto end = std::chrono::steady_clock::now();
        double duration = std::chrono::duration<double, std::micro>(end - start).count();
        printf("Load duration:%.2lfus\n", duration);
    }
    free(data);
}

void warm_up()
{
    int cnt = 0;
    int data[1024];
    while (cnt++ < 100)
    {
        for (int i = 0; i < 1024; i++)
        {
            data[i] = rand();
        }
        std::sort(data, data + 1024);
    }
}

} // namespace sort
int main()
{
    using namespace sort;
    warm_up();
    // test_stdsort();
    test_insert_sort();
    return 0;
}
