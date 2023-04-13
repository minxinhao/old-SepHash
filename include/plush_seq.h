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

namespace PlushSeq
{

/*
*   Plush源码中每个Bucket大小为16*16 = 256 bytes，每次Migrate和Search都以bucket为单位进行读取，在DM环境下这样性能较差
*   尝试对Bottom Level中InnerGroup的空间进行连续分配（已经满足），并在migrate和search时进行大粒度的读取
*/

/*
* TODO : 暂时懒得写了
*/

// Plush代码中使用了ENTRY_IDX + BUCKET_IDX的配置，实际上就是增大了初始HashTable大小；
// 这里为了保证公平性，和其他HashTable的初始大小保持一致
// 还是不行；Plush使用了类似Bucket List又不太像的方式；
// ENTRY_IDX对应一组BUCKET，这一组中任何一个BUCKET满了之后就进行rehash

// number of groups in first level
// 为了和其他table初始大小保持一致：((1+16)*110*16)/(16*16) = 117 ~ 128 = 1<<7
constexpr uint64_t init_group_bits = 7; 
constexpr uint64_t init_group_num = (1 << init_group_bits); 
// number of bucket in every group
constexpr uint64_t bucket_per_group = (1 << 4);
// number of entry in every bucket
constexpr uint64_t entry_per_bucket = 16;
// number of entry in every group
constexpr uint64_t entry_per_group = entry_per_bucket * bucket_per_group;
// 固定fanout为2，论文也说的2，不知道为啥代码里面用的16
constexpr uint64_t fanout = 2;

// constexpr uint64_t max_level = 16;
constexpr uint64_t max_level = 10;
constexpr uint64_t max_group_size = (1<<max_level)*init_group_num;
// (total_key_num) / (entry_per_bucket * bucket_per_group) = (11000000)/(16*16) = 43000
// constexpr uint64_t max_group_size = 43000;

// 64KB的dev mem，用作lock
constexpr uint64_t dev_mem_size = (1 << 10) * 64;
// aligned size of len bitfield in DepSlot
constexpr uint64_t ALIGNED_SIZE = 64;

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
    void print(const char* desc = nullptr){
        if(desc!=nullptr) log_err("%s klen:%lu key:%lu vlen:%lu value:%s",desc,k_len,*(uint64_t*)data,v_len,data+sizeof(uint64_t));
        else log_err("klen:%lu key:%lu vlen:%lu value:%s",k_len,*(uint64_t*)data,v_len,data+sizeof(uint64_t));
    }
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
    uint8_t len : 8;
    uint8_t fp : 8;
    uint64_t offset : 48;
    operator uint64_t()
    {
        return *(uint64_t *)this;
    }
    void print(const char* desc = nullptr) const{
        if(desc!=nullptr) log_err("%s len:%d fp:%x offset:%lx",desc,len,fp,offset);
        else log_err("len:%d fp:%x offset:%lx",len,fp,offset);
    }
} __attribute__((aligned(1)));

struct Bucket{
    Entry entrys[entry_per_bucket];
} __attribute__((aligned(1)));

struct BucketPointer{
    __uint128_t filter; // 存放bloom filter计算得到的数组
    uintptr_t buc_ptr;
}__attribute__((aligned(1)));;

struct InnerGroupPointer{
    uint64_t size ;
    uint64_t epoch ;
    BucketPointer bucket_pointers[bucket_per_group];
}__attribute__((aligned(1)));

struct TopPointer{
    uint64_t lock;
    uint64_t epoch;
    uint64_t size[bucket_per_group];
    // Top Bucket is At fixed postion
}__attribute__((aligned(1)));

// Bucket Array连续存放在Directory之后，提前预留好空间
struct Directory
{
    // 当前HashTable层数，从1开始
    uint64_t cur_level ; 
    TopPointer first_level[init_group_num];
    InnerGroupPointer bottom_levels[max_group_size];
    // 为多客户端同步保留的字段，不影响原有空间布局
    uint64_t start_cnt;
    void print(const char* desc = nullptr){
        log_err("==Print Dir===");
        for(uint64_t i = 0 ; i < init_group_num ; i++){
            log_err("Group:%lu lock:%lx epoch:%lu",i,first_level[i].lock,first_level[i].epoch);
        }
    }
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
    task<> migrate_top(uint64_t group_id);
    task<> migrate_bot(uint64_t source_level,uint64_t group_cnt,uint64_t group_id,uintptr_t group_ptr,uintptr_t buc_start_ptr);
    task<> rehash(Bucket *bucket, uint64_t size, uint64_t level, uint64_t* keys, Entry *new_entrys, uint64_t *sizes);
    task<> bulk_level_insert(uint64_t level, uint64_t epoch, const uint64_t *keys,const Entry *new_entrys, const uint64_t *sizes);
    
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