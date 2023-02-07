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

namespace RDMA_BENCH
{
constexpr uint64_t INIT_DEPTH = 4;
constexpr uint64_t MAX_DEPTH = 14;
constexpr uint64_t DIR_SIZE = (1 << MAX_DEPTH);
constexpr uint64_t dev_mem_size = (1 << 10) * 64; // 64KB的dev mem，用作lock
constexpr uint64_t num_lock =
    (dev_mem_size - sizeof(uint64_t)) / sizeof(uint64_t); // Lock数量，client对seg_id使用hash来共享lock

struct Slot
{
    uint8_t fp : 8;
    uint8_t len : 3;
    uint8_t sign : 1; // 用来表示split delete信息
    uint8_t dep : 4;
    uint64_t offset : 48;
}__attribute__((aligned(1)));

struct Slice
{
    uint64_t len;
    char *data;
};

/// @brief 记录MainSeg中，偏差超过32处的fp和offset
struct FpInfo{ 
    uint8_t num; // 数量 
}__attribute__((aligned(1)));

struct DirEntry
{
    // TODO : 实际上只需要用5 bits，为了方便ptr统一48，所以这里仍保留16bits
    uint64_t local_depth ; 
    uintptr_t cur_seg_ptr ;
    uintptr_t main_seg_ptr ;
    uint64_t main_seg_len ;
} __attribute__((aligned(8)));

struct Directory
{
    uint64_t global_depth;   // number of segment
    DirEntry segs[DIR_SIZE]; // Directory use MSB and is allocated enough space in advance.
    uint64_t start_cnt;      // 为多客户端同步保留的字段，不影响原有空间布局
} __attribute__((aligned(8)));

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