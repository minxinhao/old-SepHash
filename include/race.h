#pragma once
#include <map>
#include <vector>
#include <math.h>
#include <fcntl.h>
#include <cassert>
#include <tuple>
#include <chrono>
#include "aiordma.h"
#include "kv_block.h"
#include "hash.h"
#include "config.h"
#include "alloc.h"

namespace RACE{

constexpr uint64_t SLOT_PER_BUCKET = 8;
constexpr uint64_t BUCKET_BITS = 6;
constexpr uint64_t BUCKET_PER_SEGMENT = 1 << (BUCKET_BITS);
constexpr uint64_t INIT_DEPTH = 4;
constexpr uint64_t MAX_DEPTH = 22;
constexpr uint64_t DIR_SIZE = (1 << MAX_DEPTH);

struct Bucket
{
    uint32_t local_depth;
    uint32_t suffix;
    Slot slots[SLOT_PER_BUCKET];
} __attribute__((aligned(8)));

struct Segment
{
    struct Bucket buckets[BUCKET_PER_SEGMENT * 3];
} __attribute__((aligned(8)));

struct DirEntry
{
    uint64_t split_lock;
    uintptr_t seg_ptr;
    uint64_t local_depth;
} __attribute__((aligned(8)));

struct Directory
{
    uint64_t resize_lock; //最后位为global-split lock，后续为local-split count
    uint64_t global_depth;
    struct DirEntry segs[DIR_SIZE]; // Directory use MSB and is allocated enough space in advance.
    uint64_t start_cnt;             // 为多客户端同步保留的字段，不影响原有空间布局
} __attribute__((aligned(8)));

class RACEClient
{
public:
    RACEClient(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id);

    RACEClient(const RACEClient &) = delete;

    ~RACEClient();

    // Used for sync operation and test
    task<> start(uint64_t total);
    task<> stop();
    task<> reset_remote();

    task<> insert(Slice *key, Slice *value);
    task<> search(Slice *key, Slice *value);
    task<> update(Slice *key, Slice *value);
    task<> remove(Slice *key);
    
private:
    task<> sync_dir();

    bool FindLessBucket(Bucket *buc1, Bucket *buc2);

    uintptr_t FindEmptySlot(Bucket *buc, uint64_t buc_idx, uintptr_t buc_ptr);

    bool IsCorrectBucket(uint64_t segloc, Bucket *buc, uint64_t pattern);

    task<int> Split(uint64_t seg_loc, uintptr_t seg_ptr, uint64_t local_depth, bool global_flag);

    // Global/Local并行的方式造成的等待冲突太高了，就使用简单的单个lock
    task<int> LockDir();
    task<> UnlockDir();
    task<int> SetSlot(uint64_t buc_ptr, uint64_t slot);
    task<> MoveData(uint64_t old_seg_ptr, uint64_t new_seg_ptr, Segment *seg, Segment *new_seg);

    // rdma structs
    rdma_client *cli;
    rdma_conn *conn;
    rdma_rmr rmr;
    struct ibv_mr *lmr;

    Alloc alloc;
    RAlloc ralloc;
    uint64_t machine_id;
    uint64_t cli_id;
    uint64_t coro_id;

    //Statistic
    Perf perf;

    // Data part
    Directory *dir;
};

class RACEServer
{
public:
    RACEServer(Config &config);
    ~RACEServer();

private:
    void Init(Directory *dir);

    rdma_dev dev;
    rdma_server ser;
    struct ibv_mr *lmr;
    char *mem_buf;

    Alloc alloc;
    Directory *dir;
};

}//namespace RACE