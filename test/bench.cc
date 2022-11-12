#include <stdlib.h>
#include <stdio.h>
#include <random>
#include <algorithm>
#include "search.h"
#include "common.h"
#include "aiordma.h"

using search_func = int(const int *, int, int);
void run(int *data, int test_num, search_func func)
{
    volatile int pos;
    auto start_time = std::chrono::steady_clock::now();
    for (int i = 0; i < test_num; i++)
    {
        pos = func(data, test_num, i);
        if (data[pos] != i)
        {
            log_err("pos:%d-data[pos]:%d expected:%d", pos, data[pos], i);
            return;
        }
    }
    auto end_time = std::chrono::steady_clock::now();
    double duration = std::chrono::duration<double, std::nano>(end_time - start_time).count();
    log_info("Search Latency per op :%.2lfns", duration / (1.0 * test_num));
    log_info("Load IOPS:%.2lfMops", (1.0 * test_num * 1000) / duration);
}

void test_linear_search()
{
    auto test = [](search_func linear_search)
    {
        for (int test_num = 8; test_num <= 128 * 1024; test_num *= 2)
        {
            log_info("test_num:%d", test_num);
            int *data = new int[test_num];
            for (int i = 0; i < test_num; i++)
            {
                data[i] = i;
            }
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(data, data + test_num, g);
            run(data, test_num, linear_search);
            delete[] data;
        }
    };
    log_info("Test Linear_Search");
    test(linear_search);
    log_info("Test Linear_Search_SSE");
    test(linear_search_sse);
    log_info("Test Linear_Search_AVX");
    test(linear_search_avx_8);
}

void test_bs()
{
    log_info("Test binsearch");
    int test_num = 128 * 1024;
    int *data = new int[test_num];
    for (int i = 0; i < test_num; i++)
    {
        data[i] = i;
    }
    run(data, test_num, binsearch);
    delete[] data;
}

void test_bsb()
{
    log_info("Test binsearch_branless");
    int test_num = 128 * 1024;
    int *data = new int[test_num];
    for (int i = 0; i < test_num; i++)
    {
        data[i] = i;
    }

    for (int i = 8; i <= test_num; i = i * 2)
    {
        log_info("test_num:%d", i);
        run(data, i, binary_search_branchless);
    }
    delete[] data;
}

void test_bss()
{
    log_info("Test binsearch_sse");
    int test_num = 128 * 1024;
    int *data = new int[test_num];
    for (int i = 0; i < test_num; i++)
    {
        data[i] = i;
    }

    run(data, test_num, binsearch_sse);
    delete[] data;
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
        op ? co_await conn->read(rmr.raddr, rmr.rkey, ((char *)lmr->addr), read_size, lmr->lkey) : co_await conn->write(rmr.raddr, rmr.rkey, ((char *)lmr->addr), read_size, lmr->lkey);
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
            op ? tasks.emplace_back(conn->read(rmr.raddr + j * read_size, rmr.rkey, ((char *)lmr->addr) + j * read_size, read_size, lmr->lkey)) : tasks.emplace_back(conn->write(rmr.raddr + j * read_size, rmr.rkey, ((char *)lmr->addr) + j * read_size, read_size, lmr->lkey));
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
    test_linear_search();
    // test_bs();
    // test_bsb();
    // test_bss();

    // Rdma
    // test_rread();
}