// #include "dhash.h"

// namespace DHash
// {
// inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
// {
//     return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
// }

// inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
// {
//     return ((pattern) & ((1 << global_depth) - 1));
// }
// inline __attribute__((always_inline)) uint64_t get_buc_loc(uint64_t pattern)
// {
//     return (pattern >> (8 * sizeof(uint64_t) - BUCKET_BITS ));
// }
// inline __attribute__((always_inline)) bool check_suffix(uint64_t suffix, uint64_t seg_loc, uint64_t local_depth)
// {
//     return ((suffix & ((1 << local_depth) - 1)) ^ (seg_loc & ((1 << local_depth) - 1)));
// }
// void PrintDir(Directory *dir);

// DHashServer::DHashServer(Config &config) : dev("mlx5_0", 1, config.roce_flag), ser(dev)
// {
//     lmr = dev.reg_mr(233, config.mem_size);
//     alloc.Set((char *)lmr->addr, lmr->length);
//     main_dir = (Directory *)alloc.alloc(sizeof(Directory));
//     memset(main_dir, 0, sizeof(Directory));
//     cur_dir = (Directory *)alloc.alloc(sizeof(Directory));
//     memset(cur_dir, 0, sizeof(Directory));
//     Init(cur_dir);
//     PrintDir(cur_dir);
//     ser.start_serve();
// }

// DHashServer::~DHashServer()
// {
//     rdma_free_mr(lmr);
// }

// void DHashServer::Init(Directory *dir)
// {
//     dir->global_depth = INIT_DEPTH;
//     dir->resize_lock = 0;
//     uint64_t dir_size = pow(2, INIT_DEPTH);
//     Segment *tmp;
//     for (uint64_t i = 0; i < dir_size; i++)
//     {
//         tmp = (Segment *)alloc.alloc(sizeof(Segment),true);
//         memset(tmp, 0, sizeof(Segment));
//         dir->segs[i].seg_ptr = (uintptr_t)tmp;
//         dir->segs[i].local_depth = INIT_DEPTH;
//         for (uint64_t j = 0; j < BUCKET_PER_SEGMENT; j++)
//         {
//             tmp->buckets[j].local_depth = INIT_DEPTH;
//             tmp->buckets[j].suffix = i;
//         }
//     }
// }

// void PrintDir(Directory *dir)
// {
//     printf("---------PrintRACE-----\n");
//     printf("Global Depth:%lu\n", dir->global_depth);
//     printf("Resize Lock :%lu\n", dir->resize_lock);
//     uint64_t dir_size = pow(2, dir->global_depth);
//     printf("dir_size :%lu\n", dir_size);
//     for (uint64_t i = 0; i < dir_size; i++)
//     {
//         printf("Segment:seg_loc:%lx lock:%lu local_depth:%lu seg_ptr:%lx\n", i, dir->segs[i].split_lock,
//                dir->segs[i].local_depth, dir->segs[i].seg_ptr);
//     }
// }

// DHashClient::DHashClient(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, uint64_t _machine_id,
//                          uint64_t _cli_id, uint64_t _coro_id)
// {
//     // id info
//     machine_id = _machine_id;
//     cli_id = _cli_id;
//     coro_id = _coro_id;
//     // rdma utils
//     cli = _cli;
//     conn = _conn;
//     lmr = _lmr;

//     // alloc info
//     alloc.Set((char *)lmr->addr, lmr->length);
//     log_info("laddr:%lx llen:%lx", (uint64_t)lmr->addr, lmr->length);
//     rmr = cli->run(conn->query_remote_mr(233));
//     log_info("raddr:%lx rlen:%lx rend:%lx", (uint64_t)rmr.raddr, rmr.rlen, rmr.raddr + rmr.rlen);
//     uint64_t rbuf_size = (rmr.rlen - (1ul << 30) * 5) /
//                          (config.num_machine * config.num_cli * config.num_coro); // 头部保留5GB，其他的留给client
//     ralloc.SetRemote(
//         rmr.raddr + rmr.rlen -
//             rbuf_size * (config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id),
//         rbuf_size, rmr.raddr, rmr.rlen);

//     // sync dir
//     main_dir = (Directory *)alloc.alloc(sizeof(Directory));
//     memset(main_dir, 0, sizeof(Directory));

//     cur_dir = (Directory *)alloc.alloc(sizeof(Directory));
//     memset(cur_dir, 0, sizeof(Directory));
//     cli->run(sync_dir(CUR_DIR_PTR, cur_dir));
// }

// DHashClient::~DHashClient()
// {
//     perf.Print();
// }

// task<> DHashClient::reset_remote()
// {
//     //模拟远端分配器信息
//     Alloc server_alloc;
//     server_alloc.Set((char *)rmr.raddr, rmr.rlen);
//     server_alloc.alloc(sizeof(Directory));
//     server_alloc.alloc(sizeof(Directory));

//     //重置远端segment
//     memset(main_dir, 0, sizeof(Directory));
//     memset(cur_dir, 0, sizeof(Directory));
//     cur_dir->global_depth = INIT_DEPTH;
//     cur_dir->resize_lock = 0;
//     cur_dir->start_cnt = 0;
//     uint64_t dir_size = pow(2, INIT_DEPTH);
//     alloc.ReSet(2 * sizeof(Directory)); // Make room for local_segment
//     Segment *local_seg = (Segment *)alloc.alloc(sizeof(Segment));
//     uint64_t remote_seg;
//     for (uint64_t i = 0; i < dir_size; i++)
//     {
//         remote_seg = (uintptr_t)server_alloc.alloc(sizeof(Segment),true);
//         memset(local_seg, 0, sizeof(Segment));
//         cur_dir->segs[i].seg_ptr = remote_seg;
//         cur_dir->segs[i].local_depth = INIT_DEPTH;
//         for (uint64_t j = 0; j < BUCKET_PER_SEGMENT; j++)
//         {
//             local_seg->buckets[j].local_depth = INIT_DEPTH;
//             local_seg->buckets[j].suffix = i;
//         }
//         co_await conn->write(remote_seg, rmr.rkey, local_seg, size_t(sizeof(Segment)), lmr->lkey);
//     }

//     //重置远端 Directory
//     co_await conn->write(rmr.raddr + MAIN_DIR_PTR, rmr.rkey, main_dir, size_t(sizeof(Directory)), lmr->lkey);
//     co_await conn->write(rmr.raddr + CUR_DIR_PTR, rmr.rkey, cur_dir, size_t(sizeof(Directory)), lmr->lkey);
// }

// task<> DHashClient::start(uint64_t total)
// {
//     uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t), true);
//     *start_cnt = 0;
//     co_await conn->fetch_add(rmr.raddr + sizeof(Directory) - sizeof(uint64_t), rmr.rkey, *start_cnt, 1);
//     // log_info("Start_cnt:%lu", *start_cnt);
//     while ((*start_cnt) < total)
//     {
//         co_await conn->read(rmr.raddr + sizeof(Directory) - sizeof(uint64_t), rmr.rkey, start_cnt, sizeof(uint64_t),
//                             lmr->lkey);
//     }
// }

// task<> DHashClient::stop()
// {
//     uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t));
//     co_await conn->fetch_add(rmr.raddr + sizeof(Directory) - sizeof(uint64_t), rmr.rkey, *start_cnt, -1);
//     // log_info("Start_cnt:%lu", *start_cnt);
//     while ((*start_cnt) != 0)
//     {
//         co_await conn->read(rmr.raddr + sizeof(Directory) - sizeof(uint64_t), rmr.rkey, start_cnt, sizeof(uint64_t),
//                             lmr->lkey);
//     }
// }

// task<> DHashClient::sync_dir(uintptr_t dir_ptr, Directory *dir)
// {
//     co_await conn->read(rmr.raddr + dir_ptr + sizeof(uint64_t), rmr.rkey, &dir->global_depth, sizeof(uint64_t),
//                         lmr->lkey);
//     co_await conn->read(rmr.raddr + dir_ptr + sizeof(uint64_t) * 2, rmr.rkey, dir->segs,
//                         (1 << dir->global_depth) * sizeof(DirEntry), lmr->lkey);
// }

// task<> DHashClient::insert(Slice *key, Slice *value)
// {
//     alloc.ReSet(2 * sizeof(Directory));
//     uint64_t pattern_1;
//     auto pattern = hash(key->data, key->len);
//     pattern_1 = (uint64_t)pattern;
//     KVBlock *kv_block = InitKVBlock(key, value, &alloc);
//     uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
//     uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
//     auto wkv = conn->write(kvblock_ptr, rmr.rkey, kv_block, kvblock_len, lmr->lkey);
//     uint64_t retry_cnt = 0;
// Retry:
//     alloc.ReSet(2 * sizeof(Directory) + kvblock_len);
//     perf.StartPerf();
//     retry_cnt++;
//     // Cal SegPos && BucPtr
//     uint64_t segloc = get_seg_loc(pattern_1, cur_dir->global_depth);
//     uintptr_t segptr = cur_dir->segs[segloc].seg_ptr;
//     log_info("[%lu:%lu]insert:%lu",cli_id,coro_id,*(uint64_t*)key->data);
//     // 1RTT: Read Bucket && Write KV-Data
//     uint64_t bucidx = get_buc_loc(pattern_1);
//     uintptr_t bucptr = segptr + bucidx * sizeof(Bucket);
//     Bucket *buc_data = (Bucket *)alloc.alloc(sizeof(Bucket));
//     auto rbuc1 = conn->read(bucptr, rmr.rkey, buc_data, sizeof(Bucket), lmr->lkey);
//     co_await std::move(rbuc1);
//     if (retry_cnt == 1)
//         co_await std::move(wkv);

//     if (cur_dir->segs[segloc].local_depth != buc_data->local_depth)
//     {
//         log_err("[%lu:%lu] insert:%lu inconsistent depth: local:%lu remote:%d",cli_id,coro_id,*(uint64_t*)key->data,cur_dir->segs[segloc].local_depth , buc_data->local_depth);
//         exit(-1);
//         co_await sync_dir(CUR_DIR_PTR, cur_dir);
//         goto Retry;
//     }
//     perf.AddPerf("ReadBuc");

//     uint64_t slot_id = linear_search((uint64_t *)buc_data->slots, SLOT_PER_BUCKET - 1, 0);
//     uintptr_t slot_ptr = bucptr + sizeof(uint64_t) + slot_id * sizeof(uint64_t);
//     if (slot_id == -1)
//     {
//         // Split
//         perf.AddCnt("SplitCnt");
//         co_await Split();
//         goto Retry;
//     }
//     // 2nd RTT: Using RDMA CAS to write the pointer of the key-value block
//     perf.StartPerf();
//     Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
//     tmp->fp = fp(pattern_1);
//     tmp->depinfo = pattern_1 >> buc_data->local_depth;
//     tmp->len = kvblock_len;
//     tmp->offset = ralloc.offset(kvblock_ptr);
//     perf.AddCnt("SlotCnt");
//     if (!co_await conn->cas_n(slot_ptr, rmr.rkey, 0, *(uint64_t *)tmp))
//     {
//         perf.AddPerf("CasSlot");
//         goto Retry;
//     }
//     perf.AddPerf("CasSlot");

//     // 3rd RTT: Re-reading bucket to remove duplicate keys
//     perf.StartPerf();
//     auto rbuc2 = conn->read(bucptr, rmr.rkey, buc_data, sizeof(Bucket), lmr->lkey);
//     co_await std::move(rbuc2);
//     perf.AddPerf("RRead");

//     // Check Dupulicate-key
//     perf.StartPerf();
//     for (uint64_t i = 0; i < SLOT_PER_BUCKET; i++)
//     {
//         if (buc_data->slots[i].fp == tmp->fp && buc_data->slots[i].len == tmp->len &&
//             buc_data->slots[i].offset != tmp->offset)
//         {
//             char *tmp_key = (char *)alloc.alloc(buc_data->slots[i].len);
//             co_await conn->read(ralloc.ptr(buc_data->slots[i].offset), rmr.rkey, tmp_key, buc_data->slots[i].len,
//                                 lmr->lkey);
//             if (memcmp(key->data, tmp_key + sizeof(uint64_t) * 2, key->len) == 0)
//             {
//                 log_err("[%lu:%lu]Duplicate-key :%lu", cli_id, coro_id, *(uint64_t *)key->data);
//                 co_await conn->cas_n(bucptr + sizeof(uint64_t) * (i + 1), rmr.rkey, *(uint64_t *)tmp, 0);
//             }
//         }
//     }
//     perf.AddPerf("CheckDuplicate");

//     // IsCorrectBucket
//     perf.StartPerf();
//     if (IsCorrectBucket(segloc, buc_data, pattern_1) == false)
//     {
//         co_await conn->cas_n(slot_ptr, rmr.rkey, *(uint64_t *)tmp, 0);
//         co_await sync_dir(CUR_DIR_PTR, cur_dir);
//         perf.AddPerf("IsCorrectBucket");
//         goto Retry;
//     }
//     perf.AddPerf("IsCorrectBucket");
// }

// bool DHashClient::IsCorrectBucket(uint64_t segloc, Bucket *buc, uint64_t pattern)
// {
//     if (buc->local_depth != cur_dir->segs[segloc].local_depth)
//     {
//         uint64_t suffix = get_seg_loc(pattern, buc->local_depth);
//         if (buc->suffix != suffix)
//             return false;
//     }
//     return true;
// }

// task<> DHashClient::Split(uint64_t segloc,uintptr_t seg_ptr){
//     // Read Segment
// }

// task<std::tuple<uintptr_t, uint64_t>> DHashClient::search(Slice *key, Slice *value)
// {
//     co_return std::make_tuple(0ull, 0);
// }

// task<> DHashClient::update(Slice *key, Slice *value)
// {
//     co_return;
// }

// task<> DHashClient::remove(Slice *key)
// {
//     co_return;
// }
// }; // namespace DHash