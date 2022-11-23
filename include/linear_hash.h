/*
 *   LinearHash :
 *   1. 在分离内存下，网络传输开销占据主要开销。
 *   RMDA的通信在us级别，而通过测试，即便是linear search，在128*1024大小的数组中，平均时间延迟也才300ns.
 *   因此可以通过提高Segment允许的混乱程度（全相连），通过消耗CS端计算能力，来换取延迟的降低以及减少split的发生。
 *   2. 网络开销中，50%来自于split时的数据传输：读取ptr对应的KV数据以及修改Segment中的ptr。
 *   为了减少扩展的数据开销，除了通过More Tolerant of confusions，期望通过本地修改Segment再应用、Append的方式来减少数据移动。
 *   ～～Delete/相比于之前对Segment进行split的操作，这里期望通过全局地址表+Append Log的方式避免数据搬迁。～～
 *
 *
 *   Structure：
 *   Segment : 1024 Slots，线性搜索空间；为了发挥Linear
 * Search的性能和减少网络传销，Segment内采用inline Key，seperate Value的方式；
 *   Segment之间按照Hash值进行划分连续分布。
 *
 *   using Segment = Slots[1024];
 *   Segment Array[level_1]; // init level; level_1 = 128, what ever
 *   Segment Array[Level_2]; // second level; level_2 = 2 * level_1 ;
 *   保证测试时，和RACE使用的初始空间大小一样; RACE初始为 16*8*64*3 = 24576;
 * AVX加速查找
 */
#pragma once
#include <stdint.h>

#include "config.h"
#include "aiordma.h"
#include "alloc.h"
#include "hash.h"
#include "perf.h"

namespace LinearHash
{

constexpr int init_group_base = 4;
constexpr int init_group_num = (1 << init_group_base);
constexpr int max_level = 17;  // 限制到这个级别和RACE的Directory开销接近
constexpr int seg_group_size = 4;
constexpr int slots_per_group = 120 ; // 留下一点空间，避免超过4KB的限制
constexpr int slots_max_threshold = 110 ; // 留下一点空间，避免超过4KB的限制
constexpr double load_factor = 1.0; //暂时默认满了之后再分裂
constexpr uint64_t dev_mem_size = (1<<10)*64; //64KB的dev mem，用作lock
constexpr uint64_t num_lock = dev_mem_size/sizeof(uint64_t); //Lock数量，client对seg_id使用hash来共享lock

struct Slice{
    uint64_t len;
    char* data;
};

struct ValPtr
{
    uint8_t sv : 8;       // start version
    uint8_t ev : 8;       // end version
    uint64_t offset : 48; // actual offset

    operator uint64_t(){
        return *(uint64_t*)this;
    }
    ValPtr(uint64_t u){
        *this = *(ValPtr*)(&u);
    }
}__attribute__((aligned(8)));

struct Slot
{
    uint64_t key;
    ValPtr val_ptr;
}__attribute__((aligned(8)));

struct Segment
{
    uint64_t cnt;   // count of inserted slots in this segment
    uint64_t level; // used for detect inconsistency in local directory
    uintptr_t next; // stored next segments that overflow segment group size
    Slot slots[slots_per_group];
}__attribute__((aligned(8)));

struct SegmentGroup
{
    //每个Group公用一个Lock，只在Insert时使用
    //Search、Delete、Update都根据level判断。
    uintptr_t seg_ptr[seg_group_size];
    uintptr_t last_overflow; //存放over-flow segments

    /*
    * ~暂时考虑~
    * 避免Overflow的一个方式是，当超出seg_ptr上限时，直接进行split；
    * 为了避免打破linear hash的顺序，可以每次split直接分配一半空间，数据的搬迁还是一次一个group按地址写入
    */
}__attribute__((aligned(8)));

struct Directory
{
    uint64_t level;
    uintptr_t next;
    SegmentGroup groups[(1 << max_level) * init_group_num];
    uint64_t start_cnt; // 为多客户端同步保留的字段，不影响原有空间布局
}__attribute__((aligned(8)));

class LHClient
{
  public:
    LHClient(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, uint64_t _machine_id, uint64_t _cli_id,
             uint64_t _coro_id);

    LHClient(const LHClient &) = delete;

    ~LHClient();

    // Used for sync operation and test
    task<> start(uint64_t total);
    task<> stop();
    task<> reset_remote();

    task<> insert(uint64_t key, Slice *value);
    task<> search(uint64_t key, Slice *value);
    task<> update(uint64_t key, Slice *value);
    task<> remove(uint64_t key);

  private:
    task<> sync_dir();

    // 运行时保存的临时变量
    uint64_t op_cnt; //保存已经进行的操作数
    rdma_future w1, w2, w3, w4; //用来保存insert操作后面的更新kv操作
    rdma_future free_lock;  //用来保存insert操作后的free_lock操作
    bool append_seg_flag; // 用来标识是否有需要co_await的遗留write操作
    rdma_future append_seg; // append new segment对应的rdma操作
    rdma_future memset_new_seg; // append new segment对应的rdma操作


    // rdma structs
    rdma_client *cli;
    rdma_conn *conn;
    rdma_conn *wo_wait_conn;
    rdma_rmr seg_rmr;
    rdma_rmr lock_rmr;
    ibv_mr *lmr; // cli的临时缓存区，用来读写远端内存

    Alloc alloc;
    RAlloc ralloc;
    uint64_t machine_id;
    uint64_t cli_id;
    uint64_t coro_id;

    // Statistic
    Perf perf;

    // Data part
    Directory *dir;
};

class LHServer
{
  public:
    LHServer(Config &config);
    ~LHServer();

  private:
    void Init();

    rdma_dev dev;
    rdma_server ser;
    ibv_mr *seg_mr;  // Data parts for Segments
    ibv_dm *lock_dm; // Locks for Segments
    ibv_mr *lock_mr;
    Alloc alloc;
    Directory *dir;
};

} // namespace LinearHash
