#include "race_op.h"

namespace RACEOP{
inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern)>>32)&((1<<8)-1));
}

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
}

RACEServer::RACEServer(Config &config) : dev(nullptr, 1, config.roce_flag), ser(dev)
{
    seg_mr = dev.reg_mr(233, config.mem_size);
    auto [dm, mr] = dev.reg_dmmr(234, dev_mem_size);
    lock_dm = dm;
    lock_mr = mr;

    alloc.Set((char *)seg_mr->addr, seg_mr->length);
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    Init();
    log_info("init");

    // Init locks
    // memset(lock_mr->addr, 0, lock_mr->length);
    char tmp[dev_mem_size]={};
    lock_dm->memcpy_to_dm(lock_dm,0,tmp,dev_mem_size);
    log_info("memset");
    
    ser.start_serve();
}

void RACEServer::Init()
{
    dir->global_depth = INIT_DEPTH;
    dir->resize_lock = 0;
    uint64_t dir_size = pow(2, INIT_DEPTH);
    Segment *tmp;
    for (uint64_t i = 0; i < dir_size; i++)
    {
        tmp = (Segment *)alloc.alloc(sizeof(Segment));
        memset(tmp, 0, sizeof(Segment));
        dir->segs[i].seg_ptr = (uintptr_t)tmp;
        dir->segs[i].local_depth = INIT_DEPTH;
        tmp->local_depth = INIT_DEPTH;
        tmp->suffix = i;
    }
}

RACEServer::~RACEServer()
{
    rdma_free_mr(seg_mr);
    rdma_free_dmmr({lock_dm, lock_mr});
}

RACEClient::RACEClient(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, uint64_t _machine_id,
                       uint64_t _cli_id, uint64_t _coro_id)
{
    // id info
    machine_id = _machine_id;
    cli_id = _cli_id;
    coro_id = _coro_id;

    // rdma utils
    cli = _cli;
    conn = _conn;
    // wo_wait_conn = _wo_wait_conn;
    lmr = _lmr;

    // alloc info
    alloc.Set((char *)lmr->addr, lmr->length);
    log_info("laddr:%lx llen:%lx", (uint64_t)lmr->addr, lmr->length);
    seg_rmr = cli->run(conn->query_remote_mr(233));
    lock_rmr = cli->run(conn->query_remote_mr(234));
    log_info("raddr:%lx rlen:%lx rend:%lx", (uint64_t)seg_rmr.raddr, seg_rmr.rlen, seg_rmr.raddr + seg_rmr.rlen);
    uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 30) * 5) /
                         (config.num_machine * config.num_cli * config.num_coro); // 头部保留5GB，其他的留给client
    ralloc.SetRemote(
        seg_rmr.raddr + seg_rmr.rlen -
            rbuf_size * (config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id),
        rbuf_size, seg_rmr.raddr, seg_rmr.rlen);

    // init tmp variable
    op_cnt = 0;

    // sync dir
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    cli->run(sync_dir());
}

RACEClient::~RACEClient()
{
    perf.Print();
}

task<> RACEClient::reset_remote()
{
    //模拟远端分配器信息
    Alloc server_alloc;
    server_alloc.Set((char *)seg_rmr.raddr, seg_rmr.rlen);
    server_alloc.alloc(sizeof(Directory));

    //重置远端segment
    memset(dir, 0, sizeof(Directory));
    dir->global_depth = INIT_DEPTH;
    dir->resize_lock = 0;
    dir->start_cnt = 0;
    uint64_t dir_size = pow(2, INIT_DEPTH);
    alloc.ReSet(sizeof(Directory)); // Make room for local_segment
    Segment *local_seg = (Segment *)alloc.alloc(sizeof(Segment));
    uint64_t remote_seg;
    for (uint64_t i = 0; i < dir_size; i++)
    {
        remote_seg = (uintptr_t)server_alloc.alloc(sizeof(Segment));
        memset(local_seg, 0, sizeof(Segment));
        dir->segs[i].seg_ptr = remote_seg;
        dir->segs[i].local_depth = INIT_DEPTH;
        local_seg->local_depth = INIT_DEPTH;
        local_seg->suffix = i;
        co_await conn->write(remote_seg, seg_rmr.rkey, local_seg, size_t(sizeof(Segment)), lmr->lkey);
    }
    //重置远端 Directory
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, dir, size_t(sizeof(Directory)), lmr->lkey);
}

task<> RACEClient::start(uint64_t total)
{
    // co_await sync_dir();
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t), true);
    *start_cnt = 0;
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, 1);
    // log_info("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) < total)
    {
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt, sizeof(uint64_t),
                            lmr->lkey);
    }
}

task<> RACEClient::stop()
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, -1);
    // log_info("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) != 0)
    {
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt, sizeof(uint64_t),
                            lmr->lkey);
    }
}

task<> RACEClient::sync_dir()
{
    co_await conn->read(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
    co_await conn->read(seg_rmr.raddr + sizeof(uint64_t) * 2, seg_rmr.rkey, dir->segs,
                        (1 << dir->global_depth) * sizeof(DirEntry), lmr->lkey);
}

task<> RACEClient::insert(Slice *key, Slice *value)
{
    perf.StartPerf();
    alloc.ReSet(sizeof(Directory));
    uint64_t pattern_1, pattern_2;
    auto pattern = hash(key->data, key->len);
    pattern_1 = (uint64_t)pattern;
    pattern_2 = (uint64_t)(pattern >> 64);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    auto wkv = conn->write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    uint64_t retry_cnt = 0;
    perf.AddPerf("InitKv");
Retry:
    perf.StartPerf();
    alloc.ReSet(sizeof(Directory) + kvblock_len);
    retry_cnt++;
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;
    uintptr_t lock_ptr = lock_rmr.raddr + (sizeof(uint64_t)) * (segloc % num_lock);

    // lock segment
    while (co_await conn->cas_n(lock_ptr, lock_rmr.rkey, 0, 1));
    perf.AddPerf("LockSeg");
    
    // read segment
    perf.StartPerf();
    Segment *tmp_segs = (Segment *)alloc.alloc(sizeof(Segment));
    co_await conn->read(segptr, seg_rmr.rkey, tmp_segs, sizeof(Segment), lmr->lkey);
    perf.AddPerf("ReadSeg");

    if (dir->segs[segloc].local_depth != tmp_segs->local_depth)
    {
        co_await sync_dir();
        goto Retry;
    }

    // find free slot
    perf.StartPerf();
    uint64_t slot_id = linear_search_avx_ur((uint64_t*)tmp_segs->slots, SLOT_PER_SEGMENT, 0);
    perf.AddPerf("FindSlot");
    if(slot_id == -1){
        log_err("[%lu:%lu]No Free Slot for key:%lu",cli_id,coro_id,*(uint64_t*)key->data);
        exit(-1);
        // Split
    }

    // write slot
    perf.StartPerf();
    Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
    tmp->fp = fp(pattern_1);
    tmp->len = kvblock_len;
    tmp->offset = ralloc.offset(kvblock_ptr);
    if(retry_cnt==1)
        co_await std::move(wkv);
    conn->pure_write(segptr+sizeof(uint64_t)+slot_id*sizeof(Slot), seg_rmr.rkey, tmp, sizeof(Slot), lmr->lkey);
    // co_await conn->write(segptr+sizeof(uint64_t)+slot_id*sizeof(Slot), seg_rmr.rkey, tmp, sizeof(Slot), lmr->lkey);
    perf.AddPerf("WriteSlot");

    // free segment
    *tmp = 0 ;
    perf.StartPerf();
    conn->pure_write(lock_ptr, lock_rmr.rkey, tmp, sizeof(uint64_t),lmr->lkey);
    // co_await conn->write(lock_ptr, lock_rmr.rkey, tmp, sizeof(uint64_t),lmr->lkey);
    perf.AddPerf("FreeSeg");
}

task<int> Split(uint64_t seg_loc, uintptr_t seg_ptr, uint64_t local_depth, bool global_flag){
    co_return 1;
}

task<std::tuple<uintptr_t, uint64_t>> RACEClient::search(Slice *key, Slice *value){
     uintptr_t slot_ptr;
    uint64_t slot;
    uint64_t cnt = 0;
Retry:
    co_return std::make_tuple(0ull, 0);
}

task<> RACEClient::update(Slice *key, Slice *value){
    char data[1024];
    Slice ret_value;
    ret_value.data = data;
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    auto wkv = conn->write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    
    uint64_t pattern_1, pattern_2;
    auto pattern = hash(key->data, key->len);
    pattern_1 = (uint64_t)pattern;
    pattern_2 = (uint64_t)(pattern >> 64);
    uint64_t cnt = 0 ;
Retry:
    alloc.ReSet(sizeof(Directory)+kvblock_len);
    // 1st RTT: Using RDMA doorbell batching to fetch two combined buckets
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;

    if (dir->segs[segloc].split_lock == 1){
        co_await sync_dir();
        goto Retry;
    }
}

task<> RACEClient::remove(Slice *key){
    char data[1024];
    Slice ret_value;
    ret_value.data = data;
    uint64_t cnt=0;
Retry:
    alloc.ReSet(sizeof(Directory));
    // 1st RTT: Using RDMA doorbell batching to fetch two combined buckets
    uint64_t pattern_1, pattern_2;
    auto pattern = hash(key->data, key->len);
    pattern_1 = (uint64_t)pattern;
    pattern_2 = (uint64_t)(pattern >> 64);
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;

    if (dir->segs[segloc].split_lock == 1){
        co_await sync_dir();
        goto Retry;
    }
}


}// namespace RACEOP