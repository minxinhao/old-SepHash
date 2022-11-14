#include "aiordma.h"
#include "common.h"
#include "nanobench.h"
#include "search.h"
#include <algorithm>
#include <random>
#include <stdio.h>
#include <stdlib.h>

using search_func = int(const uint64_t *, int, uint64_t);
void switch_other_op()
{
    //反复shuffle和sort好了
    auto start_time = std::chrono::steady_clock::now();
    uint64_t data[128 * 1024];
    uint64_t data_size =  128 * 1024;
    for (uint64_t i = 0; i < data_size; i++)
    {
        data[i] = i;
    }
    for(int i = 0 ; i < 16 ; i++ ){
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(data, data + data_size, g);
        std::sort(data, data + data_size);
    }
}

void run(uint64_t *data, uint64_t test_num, search_func func, bool warm_up, bool switch_flag)
{
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
        if (warm_up)
        {
            start_num = 8;
            up_num = 128 * 1024;
        }
        else
        {
            start_num = 8;
            up_num = 128 * 1024;
            // start_num = up_num = 256;
        }
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
            run(data, start_num, linear_search, warm_up, switch_flag);
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
    test(linear_search_avx_ur<1024>);
}

/// @brief
/// @param conn
/// @param lmr
/// @param read_size
/// @param op : 0-write 1-read
/// @return
task<> rread(const char *desc, rdma_conn *conn, ibv_mr *lmr, uint64_t read_size, int op)
{
    uint64_t test_num = 10000;
    auto rmr = co_await conn->query_remote_mr(233);
    auto start_time = std::chrono::steady_clock::now();
    for (int i = 0; i < test_num; i++)
    {
        op ? co_await conn->read(rmr.raddr, rmr.rkey, ((char *)lmr->addr), read_size, lmr->lkey)
           : co_await conn->write(rmr.raddr, rmr.rkey, ((char *)lmr->addr), read_size, lmr->lkey);
    }
    auto end_time = std::chrono::steady_clock::now();
    double duration = std::chrono::duration<double, std::micro>(end_time - start_time).count();
    log_info("Read Latency per op %s :%.2lfus", desc, duration / (1.0 * test_num));
    log_info("%s Load IOPS:%.2lfKops", desc, (1.0 * test_num * 1000) / duration);
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
    uint64_t test_num = 10000;
    auto rmr = co_await conn->query_remote_mr(233);
    auto start_time = std::chrono::steady_clock::now();
    for (int i = 0; i < test_num; i++)
    {
        std::vector<rdma_future> tasks;
        for (int j = 0; j < batch_size; j++)
        {
            op ? tasks.emplace_back(conn->read(rmr.raddr + j * read_size, rmr.rkey, ((char *)lmr->addr) + j * read_size,
                                               read_size, lmr->lkey))
               : tasks.emplace_back(conn->write(rmr.raddr + j * read_size, rmr.rkey,
                                                ((char *)lmr->addr) + j * read_size, read_size, lmr->lkey));
        }
        co_await std::move(tasks.back());
        for (size_t i = 0; i < tasks.size() - 1; i++)
        {
            co_await std::move(tasks[i]);
        }
    }
    auto end_time = std::chrono::steady_clock::now();
    double duration = std::chrono::duration<double, std::micro>(end_time - start_time).count();
    log_info("Read Latency per op %s :%.2lfus", desc, duration / (1.0 * test_num * batch_size));
    log_info("%s Load IOPS:%.2lfKops", desc, (1.0 * test_num * batch_size * 1000) / duration);
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
    for (uint64_t read_size = 64; read_size <= (1 << 20) * 16; read_size *= 2)
    {
        log_info("read %lu single", read_size);
        rdma_cli->run(rread("single read", conn, lmr, read_size, 1));
        log_info("read %lu*4", read_size);
        rdma_cli->run(rread("batch read", conn, lmr, read_size, 4, 1));

        log_info("write %lu single", read_size);
        rdma_cli->run(rread("single write", conn, lmr, read_size, 1));
        log_info("write %lu*4", read_size);
        rdma_cli->run(rread("batch write", conn, lmr, read_size, 4, 1));
    }

    delete conn;
    delete rdma_cli;
    rdma_free_mr(lmr);
    rdma_free_mr(rmr);
}

int main()
{
    // Search
    test_linear_search(true, false); // 第一次用来warm up ， 暂时不清楚为啥第一次性能运行这么差
    // switch_other_op(); // 换用无关代码，看是否能warm up
    test_linear_search(false, true);

    // Rdma
    // test_rread();
}