#include "aiordma.h"
#include "common.h"
#include "nanobench.h"
#include "search.h"
#include <algorithm>
#include <random>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

using search_func = int(const uint64_t *, int, uint64_t);
void switch_other_op()
{
    // 反复shuffle和sort好了
    auto start_time = std::chrono::steady_clock::now();
    uint64_t data[128 * 1024];
    uint64_t data_size = 128 * 1024;
    for (uint64_t i = 0; i < data_size; i++)
    {
        data[i] = i;
    }
    for (int i = 0; i < 16; i++)
    {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(data, data + data_size, g);
        std::sort(data, data + data_size);
    }
}

void run_search(uint64_t *data, uint64_t test_num, search_func func, bool warm_up, bool switch_flag)
{
    // 手动warm up
    // for (uint64_t i = 0; i < 1024*256; i++){
    //     linear_search_avx(data,test_num,i);
    // }
    volatile int pos;
    double duration = 0.0;
    auto start_time = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < test_num; i++)
    {
        pos = func(data, test_num, i);
        if (data[pos] != i)
        {
            log_err("pos:%d-data[pos]:%lu expected:%lu", pos, data[pos], i);
            return;
        }
    }
    auto end_time = std::chrono::steady_clock::now();
    duration += std::chrono::duration<double, std::nano>(end_time - start_time).count();
    if (!warm_up)
    {
        log_info("Search Latency per op :%.2lfns", duration / (1.0 * test_num));
        log_info("Load IOPS:%.2lfMops", (1.0 * test_num * 1000) / duration);
    }
}

void test_linear_search(bool warm_up, bool switch_flag)
{
    auto test = [=](search_func linear_search) {
        int start_num, up_num;
        // start_num = 16;
        // up_num = 128 * 1024;
        start_num = up_num = 254;
        for (; start_num <= up_num; start_num *= 2)
        {
            if (!warm_up)
                log_info("start_num:%d", start_num);
            uint64_t *data = new uint64_t[start_num];
            for (uint64_t i = 0; i < start_num; i++)
            {
                data[i] = i;
            }
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(data, data + start_num, g);
            run_search(data, start_num, linear_search, warm_up, switch_flag);
            delete[] data;
            if (switch_flag)
                switch_other_op();
        }
    };
    if (!warm_up)
        log_info("Test Linear_Search");
    test(linear_search);
    if (!warm_up)
        log_info("Test Linear_Search_AVX");
    test(linear_search_avx);
    if (!warm_up)
        log_info("Test Linear_Search_avx_16");
    test(linear_search_avx_16);
    if (!warm_up)
        log_info("Test Linear_Search_avx_ur");
    test(linear_search_avx_ur);
}

/// @brief
/// @param conn
/// @param lmr
/// @param read_size
/// @param op : 0-write 1-read
/// @return
task<> rread(const char *desc, rdma_conn *conn, ibv_mr *lmr, uint64_t read_size, int op)
{
    uint64_t test_num = 1000000;
    auto rmr = co_await conn->query_remote_mr(233);
    auto start_time = std::chrono::steady_clock::now();
    for (int i = 0; i < test_num; i++)
    {
        op ? co_await conn->read(rmr.raddr + (i % 1000) * read_size, rmr.rkey,
                                 ((char *)lmr->addr) + (i % 1000) * read_size, read_size, lmr->lkey)
           : co_await conn->write(rmr.raddr + (i % 1000) * read_size, rmr.rkey,
                                  ((char *)lmr->addr) + (i % 1000) * read_size, read_size, lmr->lkey);
    }
    auto end_time = std::chrono::steady_clock::now();
    double duration = std::chrono::duration<double, std::micro>(end_time - start_time).count();
    printf("Read Latency per op %s :%.2lfus\n", desc, duration / (1.0 * test_num));
    printf("%s Load IOPS:%.2lfKops\n", desc, (1.0 * test_num * 1000) / duration);
    fflush(stdout);
}

/// @brief
/// @param conn
/// @param lmr
/// @param read_size
/// @param batch_size
/// @param op : 0-write 1-read
/// @return
task<> rread(const char *desc, rdma_conn *conn, ibv_mr *lmr, uint64_t read_size, uint64_t batch_size, int op)
{
    uint64_t test_num = 1000000;
    auto rmr = co_await conn->query_remote_mr(233);
    auto start_time = std::chrono::steady_clock::now();
    for (int i = 0; i < test_num; i++)
    {
        std::vector<rdma_future> tasks;
        for (int j = 0; j < batch_size; j++)
        {
            op ? tasks.emplace_back(conn->read(rmr.raddr + ((i * batch_size + j) % 1000) * read_size, rmr.rkey,
                                               ((char *)lmr->addr) + ((i * batch_size + j) % 1000) * read_size,
                                               read_size, lmr->lkey))
               : tasks.emplace_back(conn->write(rmr.raddr + ((i * batch_size + j) % 1000) * read_size, rmr.rkey,
                                                ((char *)lmr->addr) + ((i * batch_size + j) % 1000) * read_size,
                                                read_size, lmr->lkey));
        }
        co_await std::move(tasks.back());
        for (size_t i = 0; i < tasks.size() - 1; i++)
        {
            co_await std::move(tasks[i]);
        }
    }
    auto end_time = std::chrono::steady_clock::now();
    double duration = std::chrono::duration<double, std::micro>(end_time - start_time).count();
    printf("Read Latency per op %s :%.2lfus\n", desc, duration / (1.0 * test_num * batch_size));
    printf("%s Load IOPS:%.2lfKops\n", desc, (1.0 * test_num * batch_size * 1000) / duration);
    fflush(stdout);
}

void test_rread()
{
    // Server
    rdma_dev dev(nullptr, 1, true);
    rdma_server ser(dev);
    ibv_mr *rmr;
    uint64_t mem_size = (1 << 20) * 100;
    rmr = dev.reg_mr(233, mem_size);
    ser.start_serve();

    // Cli
    ibv_mr *lmr = dev.create_mr(mem_size);
    rdma_client *rdma_cli = new rdma_client(dev);
    rdma_conn *conn = rdma_cli->connect("192.168.1.44");

    // Read
    for (uint64_t batch_size = 2; batch_size <= 16; batch_size *= 2)
    {
        for (uint64_t read_size = 64; read_size < (1 << 20) * 1; read_size *= 2)
        {
            printf("read %lu single\n", read_size);
            rdma_cli->run(rread("single read\n", conn, lmr, read_size, 1));
            printf("read %lu*%lu\n", read_size, batch_size);
            rdma_cli->run(rread("batch read\n", conn, lmr, read_size, batch_size, 1));

            // printf("write %lu single\n", read_size);
            // rdma_cli->run(rread("single write\n", conn, lmr, read_size, 0));
            // printf("write %lu*%lu\n", read_size,batch_size);
            // rdma_cli->run(rread("batch write\n", conn, lmr, read_size, batch_size, 0));
        }
    }

    delete conn;
    delete rdma_cli;
    rdma_free_mr(lmr);
    rdma_free_mr(rmr);
}

void test_cas(uint64_t num_cli, uint64_t num_coro)
{
    // Server
    rdma_dev dev(nullptr, 1, true);
    rdma_server ser(dev);
    ibv_mr *rmr;
    uint64_t mem_size = (1 << 20) * 100;
    rmr = dev.reg_mr(233, mem_size);
    ser.start_serve();

    // Cli
    constexpr uint64_t dev_mem_size = (1 << 10) * 64;              // 64KB的dev mem，用作lock
    constexpr uint64_t num_lock = dev_mem_size / sizeof(uint64_t); // Lock数量，client对seg_id使用hash来共享lock
    auto [lock_dm, lock_mr] = dev.reg_dmmr(234, dev_mem_size);
    uint64_t cbuf_size = (1ul << 20) * 250;
    char *mem_buf = (char *)malloc(cbuf_size);
    ibv_mr *lmr = dev.create_mr(cbuf_size, mem_buf);
    std::vector<rdma_client *> rdma_clis(num_cli, nullptr);
    std::vector<rdma_conn *> rdma_conns(num_cli, nullptr);
    std::thread ths[80];

    for (uint64_t i = 0; i < num_cli; i++)
    {
        rdma_clis[i] = new rdma_client(dev, so_qp_cap, rdma_default_tempmp_size);
        rdma_conns[i] = rdma_clis[i]->connect("192.168.1.44");
        assert(rdma_conns[i] != nullptr);
    }

    printf("CAS with %lu cli %lu coro\n", num_cli, num_coro);
    uint64_t num_op = 1000000;
    auto _rmr = rdma_clis[0]->run(rdma_conns[0]->query_remote_mr(233));
    auto lock_rmr = rdma_clis[0]->run(rdma_conns[0]->query_remote_mr(234));
    bool dm_flag = true;
    bool seq_flag = true;
    auto test_cas = [&](ibv_mr *lmr, rdma_conn *conn, uint64_t cli_id, uint64_t coro_id) -> task<> {
        auto __rmr = dm_flag ? lock_rmr : _rmr;
        uint64_t ptr = (uint64_t)__rmr.raddr;
        ptr += 32;
        ptr &= ~7;
        for (uint64_t i = 0; i < num_op; i++)
        {
            if (!seq_flag)
            {
                co_await conn->cas_n(ptr, __rmr.rkey, 0, 1);
                co_await conn->cas_n(ptr, __rmr.rkey, 1, 0);
            }
            else
            {
                if (!dm_flag)
                {
                    co_await conn->cas_n(ptr + (cli_id * num_coro + coro_id) * 8, __rmr.rkey, 0, 1);
                    co_await conn->cas_n(ptr + (cli_id * num_coro + coro_id) * 8, __rmr.rkey, 1, 0);
                }
                else
                {
                    co_await conn->cas_n(ptr + ((cli_id * num_coro + coro_id) % num_lock) * 8, __rmr.rkey, 0, 1);
                    co_await conn->cas_n(ptr + ((cli_id * num_coro + coro_id) % num_lock) * 8, __rmr.rkey, 1, 0);
                }
            }
        }
    };
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < num_cli; i++)
    {
        auto th = [=](rdma_client *rdma_cli, uint64_t cli_id) {
            std::vector<task<>> tasks;
            for (uint64_t j = 0; j < num_coro; j++)
            {
                tasks.emplace_back(test_cas(lmr, rdma_conns[i], i, j));
            }
            rdma_cli->run(gather(std::move(tasks)));
        };
        ths[i] = std::thread(th, rdma_clis[i], i);
    }
    for (uint64_t i = 0; i < num_cli; i++)
    {
        ths[i].join();
    }
    auto end = std::chrono::steady_clock::now();
    double op_cnt = 1.0 * num_op * num_cli * num_coro;
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    printf("CAS duration:%.2lfms\n", duration);
    printf("CAS IOPS:%.2lfKops\n", op_cnt / duration);

    rdma_free_mr(rmr);
    free(mem_buf);
    rdma_free_mr(lmr, false);
    for (uint64_t i = 0; i < num_cli; i++)
    {
        delete rdma_conns[i];
        delete rdma_clis[i];
    }
    rdma_free_dmmr({lock_dm, lock_mr});
}

void test_pure_write()
{
    // Server
    rdma_dev dev(nullptr, 1, true);
    rdma_server ser(dev);
    ibv_mr *rmr;
    uint64_t mem_size = (1 << 20) * 100;
    rmr = dev.reg_mr(233, mem_size);
    ser.start_serve();

    // Cli
    ibv_mr *lmr = dev.create_mr(mem_size);
    rdma_client *rdma_cli = new rdma_client(dev);
    rdma_conn *conn = rdma_cli->connect("192.168.1.44");
    rdma_conn *wo_wait_conn = rdma_cli->connect("192.168.1.44");

    // write
    uint64_t batch = 100000;
    uint64_t test_num = 63;
    uint64_t write_len = sizeof(uint64_t);
    auto pure_write = [=]() -> task<> {
        uint64_t cnt = 0;
        while (cnt < batch)
        {
            for (uint64_t i = 0; i < test_num; i++)
            {
                wo_wait_conn->pure_write((uint64_t)rmr->addr + i * write_len, rmr->rkey,
                                         ((char *)lmr->addr) + i * write_len, write_len, lmr->lkey);
            }

            // 必须对同一个connection使用一次co_await才能poll_cq
            co_await wo_wait_conn->write((uint64_t)rmr->addr, rmr->rkey, lmr->addr, write_len, lmr->lkey);
            cnt++;
        }
    };
    auto start = std::chrono::steady_clock::now();
    rdma_cli->run(pure_write());
    auto end = std::chrono::steady_clock::now();
    double op_cnt = 1.0 * batch * (test_num + 1);
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    printf("CAS duration:%.2lfms\n", duration);
    printf("CAS IOPS:%.2lfKops\n", op_cnt / duration);

    delete conn;
    delete rdma_cli;
    rdma_free_mr(lmr);
    rdma_free_mr(rmr);
}

/// @brief
/// @param num_cli
/// @param num_coro
/// @param op_type : 0:write, 1:read
void test_rdma_iops(uint64_t num_cli, uint64_t num_coro, uint64_t op_type, uint64_t read_size, uint64_t batch_size)
{
    // Server
    rdma_dev dev(nullptr, 1, true);
    rdma_server ser(dev);
    ibv_mr *rmr;
    uint64_t mem_size = (1 << 20) * 250;
    rmr = dev.reg_mr(233, mem_size);
    ser.start_serve();

    // Cli
    uint64_t cbuf_size = (1ul << 20) * 250;
    char *mem_buf = (char *)malloc(cbuf_size);
    ibv_mr *lmr = dev.create_mr(cbuf_size, mem_buf);
    std::vector<rdma_client *> rdma_clis(num_cli, nullptr);
    std::vector<rdma_conn *> rdma_conns(num_cli, nullptr);
    std::thread ths[80];
    for (uint64_t i = 0; i < num_cli; i++)
    {
        rdma_clis[i] = new rdma_client(dev, so_qp_cap, rdma_default_tempmp_size);
        rdma_conns[i] = rdma_clis[i]->connect("192.168.1.44");
        assert(rdma_conns[i] != nullptr);
    }

    printf("%s with %lu cli %lu coro\n", op_type ? "read" : "write", num_cli, num_coro);
    uint64_t num_op = 1000000;
    auto cli_rmr = rdma_clis[0]->run(rdma_conns[0]->query_remote_mr(233));
    bool seq_flag = true;
    auto test_op = [&](ibv_mr *lmr, rdma_conn *conn, uint64_t cli_id, uint64_t coro_id) -> task<> {
        for (uint64_t i = 0; i < num_op; i++)
        {
            std::vector<rdma_future> tasks;
            for (int j = 0; j < batch_size; j++)
            {
                if (!seq_flag)
                {
                    op_type ? tasks.emplace_back(
                                  conn->read(cli_rmr.raddr, cli_rmr.rkey, ((char *)lmr->addr), read_size, lmr->lkey))
                            : tasks.emplace_back(
                                  conn->write(cli_rmr.raddr, cli_rmr.rkey, ((char *)lmr->addr), read_size, lmr->lkey));
                }
                else
                {
                    op_type ? tasks.emplace_back(conn->read(cli_rmr.raddr + ((i + j) % 1000) * read_size, cli_rmr.rkey,
                                                            ((char *)lmr->addr) + ((i + j) % 1000) * read_size,
                                                            read_size, lmr->lkey))
                            : tasks.emplace_back(conn->write(cli_rmr.raddr + ((i + j) % 1000) * read_size, cli_rmr.rkey,
                                                             ((char *)lmr->addr) + ((i + j) % 1000) * read_size,
                                                             read_size, lmr->lkey));
                }
            }
            co_await std::move(tasks.back());
            for (size_t j = 0; j < tasks.size() - 1; j++)
            {
                co_await std::move(tasks[j]);
            }
        }
    };
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < num_cli; i++)
    {
        auto th = [=](rdma_client *rdma_cli, uint64_t cli_id) {
            std::vector<task<>> tasks;
            for (uint64_t j = 0; j < num_coro; j++)
            {
                tasks.emplace_back(test_op(lmr, rdma_conns[i], i, j));
            }
            rdma_cli->run(gather(std::move(tasks)));
        };
        ths[i] = std::thread(th, rdma_clis[i], i);
    }
    for (uint64_t i = 0; i < num_cli; i++)
    {
        ths[i].join();
    }
    auto end = std::chrono::steady_clock::now();
    double op_cnt = 1.0 * num_op * num_cli * num_coro;
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    printf("%s duration:%.2lfms\n", op_type ? "read" : "write", duration);
    printf("%s IOPS:%.2lfKops\n", op_type ? "read" : "write", op_cnt / duration);

    rdma_free_mr(rmr);
    free(mem_buf);
    rdma_free_mr(lmr, false);
    for (uint64_t i = 0; i < num_cli; i++)
    {
        delete rdma_conns[i];
        delete rdma_clis[i];
    }
}

int main()
{
    // Search
    // test_linear_search(true, false); // 第一次用来warm up ， 暂时不清楚为啥第一次性能运行这么差
    // // switch_other_op(); // 换用无关代码，看是否能warm up
    // test_linear_search(false, false);

    // Rdma
    // test_rread();
    // test cas
    // for (uint64_t i = 1; i <= 16; i *= 2)
    // {
    //     for (uint64_t j = 1; j <= 4; j++)
    //     {
    // test_cas(i, j);
    //     }
    // }

    // test rdma iops
    for (uint64_t size = 64; size <= (1 << 10) * 64; size *= 2)
    {
        for (uint64_t batch = 1; batch <= 16; batch *= 2)
        {
            printf("size:%lu with batch:%lu\n", size, batch);
            for (uint64_t i = 1; i <= 16; i *= 2)
            {
                for (uint64_t j = 1; j <= 4; j++)
                {
                    test_rdma_iops(i, j, 1, size, batch);
                    fflush(stdout);
                }
            }
        }
    }
    // test_pure_write();
}