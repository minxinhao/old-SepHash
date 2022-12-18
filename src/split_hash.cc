#include "split_hash.h"
namespace SPLIT_HASH
{

inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
}

Server::Server(Config &config) : dev(nullptr, 1, config.roce_flag), ser(dev)
{
    seg_mr = dev.reg_mr(233, config.mem_size);
    auto [dm, mr] = dev.reg_dmmr(234, dev_mem_size);
    lock_dm = dm;
    lock_mr = mr;

    alloc.Set((char *)seg_mr->addr, seg_mr->length);
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    Init();
    log_err("init");

    // Init locks
    // memset(lock_mr->addr, 0, lock_mr->length);
    char tmp[dev_mem_size] = {};
    lock_dm->memcpy_to_dm(lock_dm, 0, tmp, dev_mem_size);
    log_err("memset");

    ser.start_serve();
}

void Server::Init()
{
    // Set MainTable to zero
    dir->global_level = INIT_LEVEL;
    dir->cur_table = (uintptr_t)alloc.alloc(sizeof(CurSeg) * (1 << dir->global_level));

    // Init CurTable
    CurSeg *cur_table = (CurSeg *)dir->cur_table;
    for (uint64_t i = 0; i < (1 << dir->global_level); i++)
    {
        memset(cur_table, 0, sizeof(CurSeg));
        cur_table->level = INIT_LEVEL;
        cur_table->sign = 1;
        cur_table++;
    }
}

Server::~Server()
{
    rdma_free_mr(seg_mr);
    rdma_free_dmmr({lock_dm, lock_mr});
}

Client::Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
               uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id)
{
    // id info
    machine_id = _machine_id;
    cli_id = _cli_id;
    coro_id = _coro_id;

    // rdma utils
    cli = _cli;
    conn = _conn;
    wo_wait_conn = _wowait_conn;
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
    ralloc.alloc(ALIGNED_SIZE); // 提前分配ALIGNED_SIZE，免得读取的时候越界

    // sync dir
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    cli->run(sync_dir());
}

Client::~Client()
{
    perf.Print();
}

task<> Client::reset_remote()
{
    // 模拟远端分配器信息
    Alloc server_alloc;
    server_alloc.Set((char *)seg_rmr.raddr, seg_rmr.rlen);
    server_alloc.alloc(sizeof(Directory));

    // 重置远端 Lock
    alloc.ReSet(sizeof(Directory)); // Make room for local_segment
    memset(dir, 0, sizeof(Directory));
    co_await conn->write(lock_rmr.raddr, lock_rmr.rkey, dir, dev_mem_size, lmr->lkey);

    // 重置远端segment
    dir->global_level = INIT_LEVEL;
    dir->cur_table = (uintptr_t)server_alloc.alloc(sizeof(CurSeg) * (1 << dir->global_level));

    CurSeg *cur_table = (CurSeg *)alloc.alloc(sizeof(CurSeg));
    memset(cur_table, 0, sizeof(CurSeg));
    cur_table->level = INIT_LEVEL;
    cur_table->sign = 1;
    for (uint64_t i = 0; i < (1 << dir->global_level); i++)
    {
        co_await conn->write(dir->cur_table + i * sizeof(CurSeg), seg_rmr.rkey, cur_table, size_t(sizeof(CurSeg)), lmr->lkey);
    }

    // 重置远端 Directory
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, dir, size_t(sizeof(Directory)), lmr->lkey);
}

task<> Client::start(uint64_t total)
{
    // co_await sync_dir();
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t), true);
    *start_cnt = 0;
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, 1);
    // log_info("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) < total)
    {
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt,
                            sizeof(uint64_t), lmr->lkey);
    }
}

task<> Client::stop()
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, -1);
    // log_err("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) != 0)
    {
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt,
                            sizeof(uint64_t), lmr->lkey);
    }
}

task<> Client::sync_dir()
{
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, &dir->global_level, 2 * sizeof(uint64_t), lmr->lkey);
    co_await conn->read(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey, dir->segs,
                        (1 << dir->global_level) * sizeof(DirEntry), lmr->lkey);
}

task<> Client::insert(Slice *key, Slice *value)
{
    perf.StartPerf();
    alloc.ReSet(sizeof(Directory));
    uint64_t pattern_1 = (uint64_t)hash(key->data, key->len);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 3;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    uint64_t retry_cnt = 0;
    perf.AddPerf("InitKv");
Retry:
    alloc.ReSet(sizeof(Directory) + kvblock_len);
    Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_level);
    uintptr_t segptr = dir->cur_table + sizeof(CurSeg) * segloc;
    uintptr_t version_ptr = lock_rmr.raddr + sizeof(uint64_t) + (sizeof(uint64_t)) * (segloc % num_lock);

    // faa version for seg
    // TODO: 改成no_wait
    perf.StartPerf();
    uint64_t *version = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    co_await conn->fetch_add(version_ptr, lock_rmr.rkey, *version, 1);
    kv_block->version = *version;
    perf.AddPerf("FaddVer");

    // read segment
    perf.StartPerf();
    CurSeg *cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
    co_await conn->read(segptr, seg_rmr.rkey, cur_seg, sizeof(CurSeg), lmr->lkey);
    perf.AddPerf("ReadSeg");

    log_err(
        "[%lu:%lu]insert key:%lu with local_depth:%lu global_depth:%lu at segloc:%lx with seg_ptr:%lx",
        cli_id, coro_id, *(uint64_t *)key->data, cur_seg->level, dir->global_level, segloc, segptr);

    // Check whether split happened on cur_table
    if (cur_seg->level != dir->global_level)
    {
        co_await sync_dir();
        goto Retry;
    }

    // find free slot
    perf.StartPerf();
    uint64_t bitmask = 1ul << 52;
    uint64_t sign = !cur_seg->sign;
    sign = sign << 52;
    uint64_t slot_id = linear_search_bitmask((uint64_t *)cur_seg->slots, SLOT_PER_SEG - 1, sign, bitmask);
    perf.AddPerf("FindSlot");
    log_err(
        "[%lu:%lu]insert key:%lu at segloc:%lx at slot:%lx for sign:%lx and cur_seg-sign:%d",
        cli_id, coro_id, *(uint64_t *)key->data, segloc, slot_id,sign,cur_seg->sign);

    if(slot_id == -1){
        // Split
        perf.AddCnt("SplitCnt");
        co_await Split(segloc, segptr, cur_seg);
        goto Retry;
    }

    // write slot
    perf.AddCnt("SlotCnt");
    perf.StartPerf();
    uint64_t dep = cur_seg->level - (cur_seg->level % 4); // 按4对齐
    tmp->dep = pattern_1 >> dep;
    tmp->fp = fp(pattern_1);
    tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
    tmp->sign = cur_seg->sign;
    tmp->offset = ralloc.offset(kvblock_ptr);
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey); // write kv
    // 这里wkv没办法wowait，会导致前一次写入没完成，后一次写入写到同一位置
    if(!co_await conn->cas_n(segptr + 2*sizeof(uint64_t) + slot_id*sizeof(Slot) , seg_rmr.rkey, cur_seg->slots[slot_id], *tmp)){
        perf.AddPerf("WriteSlot");
        goto Retry;
    }
    perf.AddPerf("WriteSlot");
}

task<> Client::Split(uint64_t seg_loc, uintptr_t seg_ptr, CurSeg *old_seg)
{
    co_return;
}

task<int> Client::LockDir(){
    co_return 0 ;
}
task<> Client::UnlockDir(){
    co_return;
}

task<> Client::search(Slice *key, Slice *value){
    co_return;
} 
task<> Client::update(Slice *key, Slice *value){
    co_return;
}
task<> Client::remove(Slice *key){
    co_return;
}

} // namespace SPLIT_HASH