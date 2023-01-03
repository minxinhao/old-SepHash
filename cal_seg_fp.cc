#include <iostream>
#include <map>
#include <stdint.h>
#include <vector>

int main()
{
    std::map<uint32_t, uint32_t> fp_map;
    for (uint8_t i = 0; i <= UINT8_MAX; i++)
    {
        fp_map[i] = 0;
        if (i == UINT8_MAX)
            break;
    }

    freopen("out.txt", "r", stdin);
    uint32_t fp;
    uint64_t sum = 0;
    while (scanf("%x", &fp) != EOF)
    {
        fp_map[fp]++;
        sum++;
    }
    double avg = (1.0 * sum) / UINT8_MAX;
    printf("sum:%lu avg:%lf\n", sum, avg);

    sum = 0;
    uint64_t predict;
    uint64_t base_off = 0;
    uint64_t base_index = 0;
    uint64_t avg_u = (uint64_t)avg;
    uint64_t buc_size = 64;
    uint64_t max_error = 32;
    uint64_t err;
    for (uint8_t i = 0; i <= UINT8_MAX; i++)
    {
        if (i == UINT8_MAX)
            break;
        if (fp_map[i] == 0)
            continue;
        predict = (i - base_index) * avg_u + base_off;
        err = (sum >= predict) ? (sum - predict) : (predict - sum);
        if (err >= max_error)
        {
            base_index = i;
            base_off = sum;
            printf("Excceed max_error with sum:%lu and predict:%lu base_index:%lu base_off:%lu\n", sum, predict,
                   base_index, base_off);
            predict = (i - base_index) * avg_u + base_off;
        }
        printf("%.2x: num:%.2u\t- sum:%.3lu\terr:%+.2ld\tpredict:%.3lu\n", i, fp_map[i], sum,
               (uint64_t)(fp_map[i] - avg), predict);
        sum += fp_map[i];
    };
}
