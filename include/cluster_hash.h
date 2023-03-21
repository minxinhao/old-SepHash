#pragma once
#include "aiordma.h"
#include "alloc.h"
#include "config.h"
#include "hash.h"
#include "kv_trait.h"
#include "perf.h"
#include "search.h"
#include <cassert>
#include <chrono>
#include <fcntl.h>
#include <map>
#include <math.h>
#include <tuple>
#include <vector>

#define WO_WAIT_WRITE

namespace CLUSTER
{

// According RACE's Evaluation : 8 slots per main or overflow bucket in DrTM cluster hashing
constexpr uint64_t BUCKET_SIZE = 8;

// 初始DIR_SIZE允许的Slot数目是SplitHash的(1+MAX_SEG_LEN)*(SLOT_PER_SEG)/(BUCKET_SIZE)
// 以 MAX_SEG_LEN:16 SLOT_PER_SEG:110为例，比例为(1+16)*110/8 = 234
constexpr uint64_t DIR_RATIO = 256;
constexpr uint64_t INIT_TABLE_SIZE = (1 << 4) * DIR_RATIO;
// DIR空间开销为(1 << 16) * 64 * 10 /(1<<20) = 40 MB
constexpr uint64_t MAX_TABLE_SIZE = (1 << 16) * DIR_RATIO;
// 可以指向bucket array的数组大小，用来在resize时存放old/new两个表的header信息
constexpr uint64_t table_num = 2; 
// Move Data时，批量读取的Bucket的数量,这里和ZERO_SIZE对齐
constexpr uint64_t bucket_batch_size = 64 * (1<<20); 


// 64KB的dev mem，用作lock
constexpr uint64_t dev_mem_size = (1 << 10) * 64;
// aligned size of len bitfield in DepSlot
constexpr uint64_t ALIGNED_SIZE = 64;
// Resize时用来清空远端bucket的空间上线
constexpr uint64_t ZERO_SIZE = 64 * (1<<20);

struct Slice
{
    uint64_t len;
    char *data;
};

struct KVBlock
{
    uint64_t k_len;
    uint64_t v_len;
    char data[0];
} __attribute__((aligned(1)));

template <typename Alloc>
    requires Alloc_Trait<Alloc, uint64_t>
KVBlock *InitKVBlock(Slice *key, Slice *value, Alloc *alloc)
{
    KVBlock *kv_block = (KVBlock *)alloc->alloc(3 * sizeof(uint64_t) + key->len + value->len);
    kv_block->k_len = key->len;
    kv_block->v_len = value->len;
    memcpy(kv_block->data, key->data, key->len);
    memcpy(kv_block->data + key->len, value->data, value->len);
    return kv_block;
}

struct Entry
{
    uint8_t valid : 1; // sign bit for deletion
    uint8_t len : 7;
    uint8_t fp : 8;
    uint64_t offset : 48;
    operator uint64_t()
    {
        return *(uint64_t *)this;
    }
} __attribute__((aligned(1)));

struct Bucket
{
    uint64_t lock; // 根据RACE2.2.2的描述，在操作Bucket之前，要对Bucket进行上锁操作,因而使用每个Bucket List的第一个Bucket中的Lock作为lock
    uintptr_t next; // 直接使用指针算了
    // uint32_t next; // 使用array内部偏移
    Entry entrys[BUCKET_SIZE];
} __attribute__((aligned(1)));

struct TableHeader
{
    uint64_t logical_num;
    // 为了节省避免读directory的时候超出64 bytes大小，对indirect_num进行省略,因为根据Drtm源码:indirect_num = logical_num / BUCKET_SIZE
    // uint64_t indirect_num; 
    uint64_t free_indirect_num;
    uintptr_t buc_start; // pointes to start of bucket array
} __attribute__((aligned(1)));

struct Directory
{
    // Size of Directory
    uint64_t dir_lock;
    uint8_t offset; // cur table header offset in below array, prev table is at offset - 1
    TableHeader tables[table_num];
    // 为多客户端同步保留的字段，不影响原有空间布局
    uint64_t start_cnt;

} __attribute__((aligned(1)));

class Client : public BasicDB
{
public:
    Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
            uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id);

    Client(const Client &) = delete;

    ~Client();

    // Used for sync operation and test
    task<> start(uint64_t total);
    task<> stop();
    task<> reset_remote();

    task<> insert(Slice *key, Slice *value);
    task<bool> search(Slice *key, Slice *value);
    task<> update(Slice *key, Slice *value);
    task<> remove(Slice *key);

private:
    // 用来在Resize的时候Move数据
    task<> move_entry(KVBlock* kv_block,Entry* entry);

    // rdma structs
    rdma_client *cli;
    rdma_conn *conn;
    rdma_conn *wo_wait_conn;
    rdma_rmr seg_rmr;
    rdma_rmr lock_rmr;
    struct ibv_mr *lmr;

    Alloc alloc;
    RAlloc ralloc;
    uint64_t machine_id;
    uint64_t cli_id;
    uint64_t coro_id;
    uint64_t key_num;
    uint64_t key_off;

    // Statistic
    Perf perf;
    uint64_t op_cnt;
    uint64_t miss_cnt;
    uint64_t retry_cnt;

    // Data part
    Directory *dir;
};

class Server : public BasicDB
{
public:
    Server(Config &config);
    ~Server();

private:
    void Init();

    rdma_dev dev;
    rdma_server ser;
    struct ibv_mr *seg_mr;
    ibv_dm *lock_dm; // Locks for Segments
    ibv_mr *lock_mr;
    char *mem_buf;

    Alloc alloc;
    Directory *dir;
};

} // namespace SPLIT_HASH