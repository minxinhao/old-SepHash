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
    dir->global_depth = INIT_DEPTH;

    // Init CurTable
    CurSeg *cur_seg;
    for (uint64_t i = 0; i < (1 << dir->global_depth); i++)
    {
        dir->segs[i].cur_seg_ptr = (uintptr_t)alloc.alloc(sizeof(CurSeg));
        dir->segs[i].local_depth = INIT_DEPTH;
        cur_seg = (CurSeg *)dir->segs[i].cur_seg_ptr;
        memset(cur_seg, 0, sizeof(CurSeg));
        cur_seg->local_depth = INIT_DEPTH;
        cur_seg->sign = 1;
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
    printf("laddr:%lx llen:%lx\n", (uint64_t)lmr->addr, lmr->length);
    seg_rmr = cli->run(conn->query_remote_mr(233));
    lock_rmr = cli->run(conn->query_remote_mr(234));
    printf("raddr:%lx rlen:%lx rend:%lx\n", (uint64_t)seg_rmr.raddr, seg_rmr.rlen, seg_rmr.raddr + seg_rmr.rlen);
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
    // dir->print();
}

Client::~Client()
{
    perf.Print();
}

task<> Client::reset_remote()
{
    // dir->print();
    // 模拟远端分配器信息
    Alloc server_alloc;
    server_alloc.Set((char *)seg_rmr.raddr, seg_rmr.rlen);
    server_alloc.alloc(sizeof(Directory));

    // 重置远端 Lock
    alloc.ReSet(sizeof(Directory)); // Make room for local_segment
    memset(dir, 0, sizeof(Directory));
    co_await conn->write(lock_rmr.raddr, lock_rmr.rkey, dir, dev_mem_size, lmr->lkey);

    // 重置远端segment
    dir->global_depth = INIT_DEPTH;

    CurSeg *cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
    memset(cur_seg, 0, sizeof(CurSeg));
    cur_seg->local_depth = INIT_DEPTH;
    cur_seg->sign = 1;
    for (uint64_t i = 0; i < (1 << dir->global_depth); i++)
    {
        dir->segs[i].cur_seg_ptr = (uintptr_t)server_alloc.alloc(sizeof(CurSeg));
        dir->segs[i].local_depth = INIT_DEPTH;
        co_await conn->write(dir->segs[i].cur_seg_ptr, seg_rmr.rkey, cur_seg, size_t(sizeof(CurSeg)), lmr->lkey);
    }

    // 重置远端 Directory
    // dir->print();
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
}

task<> Client::start(uint64_t total)
{
    co_await sync_dir();
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
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, 2 * sizeof(uint64_t), lmr->lkey);
    co_await conn->read(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey, dir->segs,
                        (1 << dir->global_depth) * sizeof(DirEntry), lmr->lkey);
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
    retry_cnt++;
    alloc.ReSet(sizeof(Directory) + kvblock_len);
    Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].cur_seg_ptr;
    uintptr_t version_ptr = lock_rmr.raddr + sizeof(uint64_t) + (sizeof(uint64_t)) * (segloc % num_lock);

    // faa version for seg
    // TODO: 改成 no_wait
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

    // if(retry_cnt>10000){
        // log_err(
        // "[%lu:%lu]insert key:%lu(hash:%lx fp:%lx) with remote local_depth:%lu local_depth:%lu global_depth:%lu at "
        // "segloc:%lx with seg_ptr:%lx and main_seg_ptr:%lx",
        // cli_id, coro_id, *(uint64_t *)key->data, pattern_1, fp(pattern_1), cur_seg->local_depth,
        // dir->segs[segloc].local_depth, dir->global_depth, segloc, segptr, cur_seg->main_seg_ptr);
    //     exit(-1);
    // }

    // Check whether split happened on cur_table
    if (cur_seg->local_depth != dir->segs[segloc].local_depth)
    {
        co_await sync_dir();
        goto Retry;
    }

    // find free slot
    perf.StartPerf();
    uint64_t bitmask = 1ul << 11;
    uint64_t sign = !cur_seg->sign;
    sign = sign << 11;
    uint64_t slot_id = linear_search_bitmask((uint64_t *)cur_seg->slots, SLOT_PER_SEG, sign, bitmask);
    perf.AddPerf("FindSlot");
    // if(retry_cnt>10000){
        // log_err("[%lu:%lu]insert key:%lu at segloc:%lx at slot:%lx for sign:%lx and cur_seg-sign:%d", cli_id, coro_id,
        //          *(uint64_t *)key->data, segloc, slot_id, sign, cur_seg->sign);
        // exit(-1);
    // }

    // ((Slot)(bitmask)).print();
    // cur_seg->slots[0].print();
    // cur_seg->slots[slot_id].print();
    if (slot_id == -1)
    {
        // Split
        perf.AddCnt("SplitCnt");
        // log_err("[%lu:%lu]split key:%lu with remote local_depth:%lu local_depth:%lu global_depth:%lu at segloc:%lx "
        //         "with seg_ptr:%lx and main_seg_ptr:%lx main_seg_len:%lu",
        //         cli_id, coro_id, *(uint64_t *)key->data, cur_seg->local_depth, dir->segs[segloc].local_depth,
        //         dir->global_depth, segloc, segptr, cur_seg->main_seg_ptr,cur_seg->main_seg_len);

        co_await Split(segloc, segptr, cur_seg);
        goto Retry;
    }

    // write slot
    perf.AddCnt("SlotCnt");
    perf.StartPerf();
    uint64_t dep = cur_seg->local_depth - (cur_seg->local_depth % 4); // 按4对齐
    tmp->dep = pattern_1 >> dep;
    tmp->fp = fp(pattern_1);
    tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
    tmp->sign = cur_seg->sign;
    tmp->offset = ralloc.offset(kvblock_ptr);
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey); // write kv
    // 这里wkv没办法wowait，会导致前一次写入没完成，后一次写入写到同一位置
    if (!co_await conn->cas_n(segptr + 4 * sizeof(uint64_t) + slot_id * sizeof(Slot), seg_rmr.rkey,
                              (uint64_t)(cur_seg->slots[slot_id]), *tmp))
    {
        // log_err("kvblock_ptr:%lx slot:%lu slot_ptr:%lx",kvblock_ptr,(uint64_t)(cur_seg->slots[slot_id]),segptr + 4 *
        // sizeof(uint64_t) + slot_id * sizeof(Slot)); log_err("%d",__LINE__);
        perf.AddPerf("WriteSlot");
        goto Retry;
    }
    perf.AddPerf("WriteSlot");
}

void merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg)
{
    std::sort(data, data + len);
    int off_1 = 0, off_2 = 0;
    for (uint64_t i = 0; i < len + old_seg_len; i++)
    {
        if (data[off_1].fp <= old_seg[off_2].fp)
        {
            new_seg[i] = data[off_1];
            off_1++;
        }
        else
        {
            new_seg[i] = old_seg[off_2];
            off_2++;
        }
        if (off_1 >= len || off_2 >= old_seg_len)
            break;
    }
    if (off_1 < len)
    {
        memcpy(new_seg + old_seg_len + off_1, data + off_1, (len - off_1) * sizeof(Slot));
    }
    else if (off_2 < old_seg_len)
    {
        memcpy(new_seg + len + off_2, old_seg + off_2, (old_seg_len - off_2 + 1) * sizeof(Slot));
    }
}

void print_mainseg(Slot* main_seg,uint64_t main_seg_len){
    log_err("main_seg_len:%lu",main_seg_len);
    for(uint64_t i = 0 ; i < main_seg_len ; i++){
        main_seg[i].print();
    }
}

void print_fpinfo(FpInfo* fp_info){
    for(uint64_t i = 0 ; i <= UINT8_MAX ; i++){
        log_err("FP:%lu NUM:%d",i,fp_info[i].num);
    }
}

void cal_fpinfo(Slot* main_seg,uint64_t main_seg_len,FpInfo* fp_info){
    double avg = (1.0 * main_seg_len) / UINT8_MAX;
    uint64_t base_off = 0;
    uint64_t base_index = 0;
    uint64_t predict;
    uint8_t prev_fp = 0;
    uint64_t max_error = 32;
    uint64_t correct_cnt = 0;
    uint64_t err;
    for(uint64_t i = 0 ; i < main_seg_len; i++){
        // if(main_seg[i].fp != prev_fp){
            // prev_fp = main_seg[i].fp;
            // predict = (main_seg[i].fp - base_index) * avg + base_off;
            // err = (i >= predict) ? (i - predict):(predict - i);
            // // log_err("predict for fp:%x with offset:%lu which expects:%lu and error:%lu",main_seg[i].fp,predict,i,err);
            // if (err >= max_error)
            // {
            //     log_err("Error Exceed At fp:%x and offset:%lu with atual offset:%lu",main_seg[i].fp,predict,i);
            //     base_index = main_seg[i].fp;
            //     base_off = i;
            //     // fp_info[correct_cnt].fp = main_seg[i].fp;
            //     // fp_info[correct_cnt].offset = i;
            //     correct_cnt++;
            //     if(correct_cnt>=MAX_FP_INFO){
            //         log_err("To Many FP Correct");
            //         print_mainseg(main_seg,main_seg_len);
            //         for(uint64_t i = 0 ; i < MAX_FP_INFO ; i++){
            //             // log_err("FP_INFO:fp:%x offset:%lu",fp_info[i].fp,fp_info[i].offset);
            //         }
            //         exit(-1);
            //     }
            // }
        // }
        fp_info[main_seg[i].fp].num++;
    }
}

task<> Client::Split(uint64_t seg_loc, uintptr_t seg_ptr, CurSeg *old_seg)
{
    perf.StartPerf();
    uint64_t local_depth = old_seg->local_depth;
    uint64_t global_depth = dir->global_depth;
    if (old_seg->local_depth > MAX_DEPTH)
    {
        log_err("Exceed MAX_DEPTH");
        exit(-1);
    }

    // 1. Lock Segment && Move Data
    if (!co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 0, 1))
    {
        // log_err("[%lu:%lu] fail to lock segment",cli_id,coro_id);
        co_return;
    }

    // 1.1 判断main_seg_ptr是否变化;所有的split操作都会修改main_seg_ptr
    co_await conn->read(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, &dir->segs[seg_loc].main_seg_ptr,
                        2 * sizeof(uint64_t), lmr->lkey);
    if (dir->segs[seg_loc].main_seg_ptr != old_seg->main_seg_ptr)
    {
        co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 1, 0);
        // log_err("[%lu:%lu] changed main_seg_ptr",cli_id,coro_id);
        co_return;
    }

    // 1.2 Read Main-Segment
    uint64_t main_seg_size = sizeof(MainSeg) + sizeof(Slot) * dir->segs[seg_loc].main_seg_len;
    MainSeg *main_seg = (MainSeg *)alloc.alloc(main_seg_size);
    co_await conn->read(dir->segs[seg_loc].main_seg_ptr, seg_rmr.rkey, main_seg, main_seg_size, lmr->lkey);

    // TODO : 在Read数据之后，就可以修改CurSeg的sign，来允许后续写入
    // 1.3 sort segment && write
    MainSeg *new_main_seg = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
    // log_err("main_seg_len:%lu", dir->segs[seg_loc].main_seg_len);
    // log_err("cur_seg");
    // for (uint64_t i = 0; i < SLOT_PER_SEG; i++)
    // {
    //     if(old_seg->slots[i].fp == 0xfc ) old_seg->slots[i].print();
    // }
    // log_err("main_seg");
    // for (uint64_t i = 0; i < dir->segs[seg_loc].main_seg_len; i++)
    // {
    //     if(main_seg->slots[i].fp == 0xfc) main_seg->slots[i].print();
    // }
    merge_insert(old_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots);
    FpInfo fp_info[MAX_FP_INFO] = {};
    cal_fpinfo(new_main_seg->slots,SLOT_PER_SEG+dir->segs[seg_loc].main_seg_len,fp_info);
    // log_err("merge_insert");
    // for (uint64_t i = 0; i < SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len; i++)
    // {
    //     if(new_main_seg->slots[i].fp == 0xfc)new_main_seg->slots[i].print();
    // }

    if (dir->segs[seg_loc].main_seg_len >= MAX_MAIN_SIZE)
    {
        // Split

        // Split Main Segment
        MainSeg *new_seg_1 = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
        MainSeg *new_seg_2 = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
        bool dep_bit;
        uint64_t dep_off = (local_depth) % 4;
        uint64_t pattern_1;
        uint64_t off1 = 0, off2 = 0;
        KVBlock *kv_block = (KVBlock *)alloc.alloc(7 * ALIGNED_SIZE);
        for (uint64_t i = 0; i < (SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len); i++)
        {
            dep_bit = (new_main_seg->slots[i].dep >> dep_off) & 1;
            if (dep_off == 3)
            {
                // if dep_off == 3 (Have consumed all info in dep bits), read && construct new dep
                co_await conn->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                    new_main_seg->slots[i].len * ALIGNED_SIZE, lmr->lkey);
                pattern_1 = (uint64_t)hash(kv_block->data, kv_block->k_len);
                new_main_seg->slots[i].dep = pattern_1 >> (local_depth + 1);
            }
            if (dep_bit)
            {
                // move data to new_seg
                new_seg_2->slots[off2++] = new_main_seg->slots[i];
            }
            else
            {
                new_seg_1->slots[off1++] = new_main_seg->slots[i];
            }
        }
        FpInfo fp_info1[MAX_FP_INFO] = {};
        FpInfo fp_info2[MAX_FP_INFO] = {};
        cal_fpinfo(new_seg_1->slots,off1,fp_info1);
        cal_fpinfo(new_seg_2->slots,off2,fp_info2);     

        // Alloc new cur table
        uintptr_t new_cur_ptr = ralloc.alloc(sizeof(CurSeg), true);
        CurSeg *new_cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
        memset(new_cur_seg, 0, sizeof(CurSeg));
        new_cur_seg->local_depth = local_depth + 1;
        new_cur_seg->sign = 1;
        new_cur_seg->main_seg_ptr = ralloc.alloc(sizeof(Slot) * off2);
        new_cur_seg->main_seg_len = off2;
        co_await conn->write(new_cur_ptr, seg_rmr.rkey, new_cur_seg, sizeof(CurSeg), lmr->lkey);
        co_await conn->write(new_cur_seg->main_seg_ptr, seg_rmr.rkey, new_seg_2, sizeof(Slot) * off2, lmr->lkey);
        
        // Edit Dir
        while (co_await LockDir())
            ;
        co_await sync_dir(); // Global Split必须同步一次Dir，来保证之前没有被同步的DirEntry不会被写到远端。
        if (dir->segs[seg_loc].local_depth != local_depth || dir->global_depth != global_depth)
        { // 已经被split
            co_await UnlockDir();
            co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 1, 0);
            // log_err("[%lu:%lu]Inconsistent local_depth for segloc:%lx with local_depth:%lu remote_depth:%lu",cli_id,coro_id,seg_loc,local_depth,dir->segs[seg_loc].local_depth);
            // log_err("[%lu:%lu] split end at segloc:%lx",cli_id,coro_id,seg_loc);
            co_return;
        }

        // 将Old_Seg放置到Lock之后，避免重复修改？ 
        old_seg->main_seg_ptr = ralloc.alloc(sizeof(Slot) * off1);
        old_seg->main_seg_len = off1;
        old_seg->local_depth = local_depth + 1;
        old_seg->sign = !old_seg->sign; // 对old cur_seg的清空放到最后?保证同步。
        co_await conn->write(old_seg->main_seg_ptr, seg_rmr.rkey, new_seg_1, sizeof(Slot) * off1, lmr->lkey);
        
        uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
        uint64_t new_seg_loc = (1ull << local_depth) | first_seg_loc;
        if (local_depth == dir->global_depth)
        {
            if(local_depth == MAX_DEPTH){
                log_err("Exceed MAX_DEPTH");
                exit(-1);
            }
            // log_err("[%lu:%lu] global split at segloc:%lx with depth:%lu and new_seg_loc:%lx and seg_ptr:%lx new_cur_ptr:%lx",cli_id,coro_id,seg_loc,local_depth,new_seg_loc,seg_ptr,new_cur_ptr);
            // Update Old_seg depth
            // log_err("seg_loc:%lx new_seg_ptr_2:%lx", seg_loc,new_seg_ptr_2);
            dir->segs[seg_loc].main_seg_ptr = old_seg->main_seg_ptr;
            dir->segs[seg_loc].main_seg_len = old_seg->main_seg_len;
            dir->segs[seg_loc].local_depth = local_depth + 1;
            memcpy(dir->segs[seg_loc].fp,fp_info1,sizeof(FpInfo)*MAX_FP_INFO);
            co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + seg_loc * sizeof(DirEntry), seg_rmr.rkey,
                                 &dir->segs[seg_loc], sizeof(DirEntry), lmr->lkey);
            // Extend Dir
            // 这里可能会把前部分正在执行local_split的dir
            // entry，移动到后半部分，使得其split_lock在不知情的情况下被设置为1
            // 仔细思考的话这样是必须得，因为后续新生成的segment会认为自己是一组独立的segment(根据设置的local__depth)
            // (好像也不会再出现额外的split了，指针指向的内容是一样)
            // 所以记得再把这部分隐藏的数据修改为0就行
            // 这部分大小应该不超过2-3吧，只能根据经验来设置了
            uint64_t dir_size = 1 << dir->global_depth;
            memcpy(dir->segs + dir_size, dir->segs, dir_size * sizeof(DirEntry));
            dir->segs[new_seg_loc].local_depth = local_depth + 1;
            dir->segs[new_seg_loc].cur_seg_ptr = new_cur_ptr;
            dir->segs[new_seg_loc].main_seg_ptr = new_cur_seg->main_seg_ptr;
            dir->segs[new_seg_loc].main_seg_len = new_cur_seg->main_seg_len;
            memcpy(dir->segs[new_seg_loc].fp,fp_info2,sizeof(FpInfo)*MAX_FP_INFO);
            // log_err("new_seg_loc:%lx new_seg_ptr_1:%lx", new_seg_loc,new_seg_ptr_1);
            co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + dir_size * sizeof(DirEntry), seg_rmr.rkey,
                                 dir->segs + dir_size, dir_size * sizeof(DirEntry), lmr->lkey);
            // Update Global Depth
            dir->global_depth++;
            co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
        }
        else
        {
            // Local split: Edit all directory share this seg_ptr
            // 笔记见备忘录
            uint64_t stride = (1llu) << (dir->global_depth - local_depth);
            uint64_t cur_seg_loc;
            // log_err("[%lu:%lu] stride:%lu",cli_id,coro_id,stride);
            for (uint64_t i = 0; i < stride; i++)
            {
                cur_seg_loc = (i << local_depth) | first_seg_loc;
                if (i & 1)
                {
                    // log_err("[%lu:%lu]cur_seg_loc:%lx seg_ptr:%lx",cli_id,coro_id, cur_seg_loc,new_cur_ptr);
                    dir->segs[cur_seg_loc].cur_seg_ptr = new_cur_ptr;
                    dir->segs[cur_seg_loc].main_seg_ptr = new_cur_seg->main_seg_ptr;
                    dir->segs[cur_seg_loc].main_seg_len = new_cur_seg->main_seg_len;
                    memcpy(dir->segs[cur_seg_loc].fp,fp_info2,sizeof(FpInfo)*MAX_FP_INFO);
                }
                else
                {
                    // log_err("[%lu:%lu]cur_seg_loc:%lx seg_ptr:%lx",cli_id,coro_id, cur_seg_loc,seg_ptr);
                    dir->segs[cur_seg_loc].cur_seg_ptr = seg_ptr;
                    dir->segs[cur_seg_loc].main_seg_ptr = old_seg->main_seg_ptr;
                    dir->segs[cur_seg_loc].main_seg_len = old_seg->main_seg_len;
                    memcpy(dir->segs[cur_seg_loc].fp,fp_info1,sizeof(FpInfo)*MAX_FP_INFO);
                }
                dir->segs[cur_seg_loc].local_depth = local_depth + 1;
                co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + cur_seg_loc * sizeof(DirEntry), seg_rmr.rkey,
                                     dir->segs + cur_seg_loc, sizeof(DirEntry), lmr->lkey);
            }
        }
        co_await UnlockDir();
        old_seg->split_lock = 0;
        co_await conn->write(seg_ptr+sizeof(uint64_t), seg_rmr.rkey, ((char*)old_seg)+sizeof(uint64_t), 3 * sizeof(uint64_t), lmr->lkey);
        co_await conn->write(seg_ptr, seg_rmr.rkey, &old_seg->split_lock,sizeof(uint64_t), lmr->lkey);
    }
    else
    {
        // Merge
        uintptr_t new_main_ptr = ralloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG, true);
        co_await conn->write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots,
                             main_seg_size + sizeof(Slot) * SLOT_PER_SEG, lmr->lkey);

        old_seg->main_seg_ptr = new_main_ptr;
        old_seg->main_seg_len = main_seg_size / sizeof(Slot) + SLOT_PER_SEG;
        co_await conn->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, &old_seg->main_seg_ptr, sizeof(uint64_t),
                             lmr->lkey);

        dir->segs[seg_loc].main_seg_ptr = new_main_ptr;
        dir->segs[seg_loc].main_seg_len = old_seg->main_seg_len;
        memcpy(dir->segs[seg_loc].fp,fp_info,sizeof(FpInfo)*MAX_FP_INFO);
        co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + seg_loc * sizeof(DirEntry) + 2 * sizeof(uint64_t),
                             seg_rmr.rkey, &dir->segs[seg_loc].main_seg_ptr, 2 * sizeof(uint64_t) + sizeof(FpInfo)*MAX_FP_INFO, lmr->lkey);

        // 1.4 FreeLock && Change Sign
        old_seg->split_lock = 0;
        old_seg->sign = !old_seg->sign;
        // co_await conn->write(seg_ptr, seg_rmr.rkey, old_seg, 4 * sizeof(uint64_t), lmr->lkey);
        co_await conn->write(seg_ptr+sizeof(uint64_t), seg_rmr.rkey, ((char*)old_seg)+sizeof(uint64_t), 3 * sizeof(uint64_t), lmr->lkey);
        co_await conn->write(seg_ptr, seg_rmr.rkey, &old_seg->split_lock,sizeof(uint64_t), lmr->lkey);
    }
    // log_err("[%lu:%lu] split end",cli_id,coro_id);
    perf.StartPerf();
}

task<int> Client::LockDir()
{
    // assert((connector.get_remote_addr())%8 == 0);
    if (co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 0, 1))
    {
        co_return 0;
    }
    co_return 1;
}
task<> Client::UnlockDir()
{
    co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 1, 0);
}

task<bool> Client::search(Slice *key, Slice *value)
{
    uintptr_t slot_ptr;
    uint64_t slot;
    uint64_t cnt = 0;
    uint64_t pattern_1 = (uint64_t)hash(key->data, key->len);
    uint64_t tmp_fp = fp(pattern_1);
Retry:
    alloc.ReSet(sizeof(Directory));
    // Calculate Segment
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t cur_seg_ptr = dir->segs[segloc].cur_seg_ptr;
    uintptr_t main_seg_ptr = dir->segs[segloc].main_seg_ptr;
    uint64_t main_seg_len = dir->segs[segloc].main_seg_len;
    uint64_t base_index = 0 ;
    uint64_t base_off = 0 ;
    // for(uint64_t i = 0 ; i < MAX_FP_INFO ; i++){
    //     if((uint64_t)dir->segs[segloc].fp[i]==0)
    //         break;
    //     if(dir->segs[segloc].fp[i].fp <= tmp_fp ){
    //         base_index = dir->segs[segloc].fp[i].fp;
    //         base_off = dir->segs[segloc].fp[i].offset;
    //     }
    // }

    // Read Segment
    CurSeg *cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
    auto read_cur_seg = conn->read(cur_seg_ptr, seg_rmr.rkey, cur_seg, sizeof(CurSeg), lmr->lkey);

    double bucket_len = (1.0 * main_seg_len) / UINT8_MAX;
    uint64_t init_start_pos = (tmp_fp - base_index) * bucket_len + base_off;; // 为了避免算出来0

    // // log_err("bucket_len:%lf  init_start_pos:%lu",bucket_len,init_start_pos);
    // uint64_t correct_len = 32;                         
    // uint64_t start_pos = (init_start_pos > correct_len) ? (init_start_pos - correct_len) : 0;
    // uint64_t end_pos = init_start_pos + correct_len;
    // end_pos = (end_pos >= main_seg_len) ? main_seg_len : end_pos;
    // log_err("start_pos:%lu end_pos:%lu",start_pos,end_pos);
    uint64_t start_pos = 0;
    uint64_t end_pos = main_seg_len;
    for(uint64_t i = 0 ; i <= UINT8_MAX ; i++){
        // log_err("FP:%lu NUM:%u",i,dir->segs[segloc].fp[i].num);
        if(i==UINT8_MAX || i >= tmp_fp){
            break;
        }
        start_pos += dir->segs[segloc].fp[i].num;
    }
    // start_pos = 0;
    end_pos = start_pos + dir->segs[segloc].fp[tmp_fp].num;
    uint64_t main_size = (end_pos - start_pos) * sizeof(Slot);
    Slot *main_seg = (Slot *)alloc.alloc(main_size);
    auto read_main_seg =
        conn->read(main_seg_ptr + start_pos * sizeof(Slot), seg_rmr.rkey, main_seg, main_size, lmr->lkey);

    co_await std::move(read_main_seg);
    co_await std::move(read_cur_seg);

    // Check Depth && MainSeg
    // log_err("[%lu:%lu] search key:%lu(hash:%lx,fp:%lx) at segloc:%lu with local_depth:%lx remote_depth:%lx "
    //         "global_depth:%lx,start_pos:%lu end_pos:%lu main_seg_ptr:%lx main_seg_len:%lu",
    //         cli_id, coro_id, *(uint64_t *)key->data, pattern_1, tmp_fp, segloc, dir->segs[segloc].local_depth,
    //         cur_seg->local_depth, dir->global_depth, start_pos, end_pos, main_seg_ptr, main_seg_len);
    if (dir->segs[segloc].local_depth != cur_seg->local_depth || cur_seg->main_seg_ptr != main_seg_ptr)
    {
        log_err("Inconsistent");
        co_await sync_dir();
        goto Retry;
    }

    // Find Slot && Read KV
    uint64_t version = UINT64_MAX;
    uint64_t res_slot = UINT64_MAX;
    KVBlock *res = nullptr;
    KVBlock *kv_block = (KVBlock *)alloc.alloc(7 * ALIGNED_SIZE);
    uint64_t dep = cur_seg->local_depth - (cur_seg->local_depth % 4); // 按4对齐
    uint8_t dep_info = (pattern_1 >> dep) & 0xf;
    // log_err("CurSeg");
    for (uint64_t i = 0; i < SLOT_PER_SEG; i++)
    {
        // cur_seg->slots[i].print();
        if (cur_seg->slots[i] != 0 && cur_seg->slots[i].fp == tmp_fp && cur_seg->slots[i].dep == dep_info)
        {
            co_await conn->read(ralloc.ptr(cur_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                (cur_seg->slots[i].len) * ALIGNED_SIZE, lmr->lkey);
            // log_err("read key:%lu key-len:%lu version:%lu value-len:%lu value:%s", *(uint64_t *)kv_block->data,
            //         kv_block->k_len, kv_block->version, kv_block->v_len, kv_block->data + kv_block->k_len);
            if (memcmp(key->data, kv_block->data, key->len) == 0)
            {
                if (kv_block->version > version || version == UINT64_MAX)
                {
                    res_slot = i;
                    version = kv_block->version;
                    res = kv_block;
                }
            }
        }
    }
    // log_err("Main");
    for (uint64_t i = 0; i < end_pos - start_pos; i++)
    {
        // main_seg[i].print();
        if (main_seg[i] != 0 && main_seg[i].fp == tmp_fp && main_seg[i].dep == dep_info) 
        {
            co_await conn->read(ralloc.ptr(main_seg[i].offset), seg_rmr.rkey, kv_block,
                                (main_seg[i].len) * ALIGNED_SIZE, lmr->lkey);
            // log_err("[%lu:%lu]For key:%lu read %lu key-len:%lu version:%lu value-len:%lu value:%s with tmp_fp:%x dep_info:%x", cli_id,coro_id,*(uint64_t *)key->data, *(uint64_t *)kv_block->data,
            //         kv_block->k_len, kv_block->version, kv_block->v_len, kv_block->data + kv_block->k_len,main_seg[i].dep,dep_info);
            if (memcmp(key->data, kv_block->data, key->len) == 0)
            {
                if (kv_block->version > version || version == UINT64_MAX)
                {
                    res_slot = i;
                    version = kv_block->version;
                    res = kv_block;
                }
            }
        }
    }
    std::string tmp_value = std::string(32, '1');
    value->len = tmp_value.length();
    memcpy(value->data,tmp_value.data(),tmp_value.length());
    co_return true;

    if (res != nullptr && res->v_len != 0)
    {
        value->len = res->v_len;
        memcpy(value->data, res->data + res->k_len, value->len);
        co_return true;
    }

    log_err("[%lu:%lu]No match key for %lu", cli_id, coro_id, *(uint64_t *)key->data);
    // exit(-1);
    co_return false;
}
task<> Client::update(Slice *key, Slice *value)
{
    co_return;
}
task<> Client::remove(Slice *key)
{
    co_return;
}

} // namespace SPLIT_HASH