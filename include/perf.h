#pragma once
//定义一些通用的结构体
#include <stdint.h>
#include <map>
#include <string>
#include <chrono>
#define USE_PERF

struct Perf
{
    //用于性能统计
    std::map<std::string,uint64_t> cnts;
    std::map<std::string,double> perfs;
    // Perfs
    // RACE
    std::vector<std::string> race_perf = {
        "ReadBuc",
        "CalPos",
        "GetLock", // Split start
        "InitBuc",
        "EditDir",
        "FreeLock",
        "ReadOldBuc", //Move Data Start
        "ReadKv",
        "MoveSlot",
        "UpdateDep", // Move Data End // Split End
        "CasSlot",
        "RRead",
        "CheckDuplicate",
        "IsCorrectBucket",
    };
    std::vector<std::string> race_cnts = {
        "SplitCnt", 
        "SlotCnt",
    };

    // RACEOP
    std::vector<std::string> raceop_perf = {
        "InitKv",
        "LockSeg",
        "ReadSeg",
        "SyncDir",
        "FindSlot",
        "ReadKv", //Split Start
        "WriteSeg",
        "LockDir",
        "UpdateDir",
        "FreeDir", //Split End
        "WriteSlot",
        "FreeSeg",
    };
    std::vector<std::string> raceop_cnts = {
        "SplitCnt", 
        "SlotCnt",
    };

    std::chrono::_V2::steady_clock::time_point start;
    std::chrono::_V2::steady_clock::time_point end;

    void StartPerf(){
#ifdef USE_PERF
        start = std::chrono::steady_clock::now();
#endif
    }

    void AddPerf(std::string&& perf){
#ifdef USE_PERF
        end = std::chrono::steady_clock::now();
        double duration = std::chrono::duration<double, std::milli>(end - start).count();
        if(perfs.count(perf)==0){
            perfs[perf] = duration;
        }else{
            perfs[perf] += duration;
        }
#endif
    }

    void AddCnt(std::string&& cnt){
#ifdef USE_PERF
        if(cnts.count(cnt)==0){
            cnts[cnt] = 0;
        }else{
            cnts[cnt]++;
        }
#endif
    }

    void Print(){
#ifdef USE_PERF
        for(auto i:race_perf){
            if(perfs.count(i))
                printf("%s cost:%lf\n",i.c_str(),perfs[i]);
        }
        for(auto i:race_cnts){
            if(cnts.count(i))
                printf("%s cnt:%lu\n",i.c_str(),cnts[i]);
        }
        // for(auto i:raceop_perf){
        //     if(perfs.count(i))
        //         printf("%s cost:%lf\n",i.c_str(),perfs[i]);
        // }
        // for(auto i:raceop_cnts){
        //     if(cnts.count(i))
        //         printf("%s cnt:%lu\n",i.c_str(),cnts[i]);
        // } 
#endif
    }

};