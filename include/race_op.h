#pragma once
#include "aiordma.h"
#include "alloc.h"
#include "config.h"
#include "hash.h"
#include "perf.h"
#include "search.h"
#include "kv_trait.h"
#include <cassert>
#include <chrono>
#include <fcntl.h>
#include <map>
#include <math.h>
#include <tuple>
#include <vector>
// #define WO_WAIT_WRITE

namespace RACEOP
{

constexpr uint64_t SEGMENT_SIZE = 2048;
constexpr uint64_t SLOT_PER_SEGMENT = (SEGMENT_SIZE - sizeof(uint64_t)) / (sizeof(uint64_t));
constexpr uint64_t INIT_DEPTH = 4;
constexpr uint64_t MAX_DEPTH = 22;
constexpr uint64_t DIR_SIZE = (1 << MAX_DEPTH);
constexpr uint64_t dev_mem_size = (1 << 10) * 64;              // 64KB的dev mem，用作lock
constexpr uint64_t num_lock = dev_mem_size / sizeof(uint64_t); // Lock数量，client对seg_id使用hash来共享lock

struct Slot
{
    uint8_t fp : 8;
    uint8_t len : 8;
    uint64_t offset : 48;
    operator uint64_t()
    {
        return *(uint64_t *)this;
    }
    Slot(uint64_t u)
    {
        *this = *(Slot *)(&u);
    }
} __attribute__((aligned(8)));

struct Slice
{
    uint64_t len;
    char *data;
};

struct KVBlock
{
    uint64_t k_len;
    uint64_t v_len;
    char data[0]; //变长数组，用来保证KVBlock空间上的连续性，便于RDMA操作
};

template <typename Alloc>
requires Alloc_Trait<Alloc, uint64_t> KVBlock *InitKVBlock(Slice *key, Slice *value, Alloc *alloc)
{
    KVBlock *kv_block = (KVBlock *)alloc->alloc(2 * sizeof(uint64_t) + key->len + value->len);
    kv_block->k_len = key->len;
    kv_block->v_len = value->len;
    memcpy(kv_block->data, key->data, key->len);
    memcpy(kv_block->data + key->len, value->data, value->len);
    return kv_block;
}

struct Segment
{
    // lock被独立放置于device memory上
    uint32_t local_depth;
    uint32_t suffix;
    Slot slots[SLOT_PER_SEGMENT];
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

class RACEClient : public BasicDB
{
  public:
    RACEClient(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn, uint64_t _machine_id,
               uint64_t _cli_id, uint64_t _coro_id);

    RACEClient(const RACEClient &) = delete;

    ~RACEClient();

    // Used for sync operation and test
    task<> start(uint64_t total);
    task<> stop();
    task<> reset_remote();

    task<> insert(Slice *key, Slice *value);
    task<std::tuple<uintptr_t, uint64_t>> search(Slice *key, Slice *value); // return slotptr
    task<> update(Slice *key, Slice *value);
    task<> remove(Slice *key);

  private:
    task<> sync_dir();

    task<int> Split(uint64_t seg_loc, uintptr_t seg_ptr, uint64_t local_depth, bool global_flag);

    // Global/Local并行的方式造成的等待冲突太高了，就使用简单的单个lock
    // task<int> LockDir();
    // task<> UnlockDir();
    // task<int> SetSlot(uint64_t buc_ptr, uint64_t slot);
    // task<> MoveData(uint64_t old_seg_ptr, uint64_t new_seg_ptr, Segment *seg, Segment *new_seg);

    // rdma structs
    rdma_client *cli;
    struct ibv_mr *lmr; // cli的临时缓存区，用来读写远端内存
    rdma_conn *conn;
    rdma_conn *wo_wait_conn; //用来发送pure_wait
    rdma_rmr seg_rmr;
    rdma_rmr lock_rmr;

    Alloc alloc;
    RAlloc ralloc;
    uint64_t machine_id;
    uint64_t cli_id;
    uint64_t coro_id;

    // 运行时保存的临时变量
    uint64_t op_cnt; //保存已经进行的操作数

    // Statistic
    Perf perf;

    // Data part
    Directory *dir;
};

class RACEServer : public BasicDB
{
  public:
    RACEServer(Config &config);
    ~RACEServer();

  private:
    void Init();

    rdma_dev dev;
    rdma_server ser;
    struct ibv_mr *seg_mr;
    ibv_dm *lock_dm; // Locks for Segments
    ibv_mr *lock_mr;

    Alloc alloc;
    Directory *dir;
};

} // namespace RACEOP