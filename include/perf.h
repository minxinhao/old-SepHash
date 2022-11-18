#pragma once
//定义一些通用的结构体
#include <stdint.h>
#include <map>
#include <string>
#include <chrono>
#include "alloc.h"
// #define USE_PERF

struct Perf
{
    //用于性能统计
    std::map<std::string,uint64_t> cnts;
    std::map<std::string,double> perfs;
    // Perfs
    const std::string ReadBuc{"ReadBuc"};
    const std::string CalPos{"CalPos"};
    const std::string GetLock{"GetLock"}; // split start
    const std::string InitBuc{"InitBuc"};
    const std::string EditDir{"EditDir"};
    const std::string FreeLock{"FreeLock"};
    const std::string ReadOldBuc{"ReadOldBuc"}; //Move Data Start
    const std::string ReadKv{"ReadKv"};
    const std::string MoveSlot{"MoveSlot"};
    const std::string UpdateDep{"UpdateDep"}; // Move Data End // split end
    const std::string CasSlot{"CasSlot"};
    const std::string RRead{"RRead"};
    const std::string CheckDuplicate{"CheckDuplicate"};
    const std::string IsCorrectBucket{"IsCorrectBucket"};

    //cnts
    const std::string SplitCnt{"SplitCnt"};
    const std::string SlotCnt{"SlotCnt"};
    

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
        printf("%s cost:%lf\n",ReadBuc.c_str(),perfs[ReadBuc]);
        printf("%s cost:%lf\n",CalPos.c_str(),perfs[CalPos]);
        printf("%s cost:%lf\n",GetLock.c_str(),perfs[GetLock]);
        printf("%s cost:%lf\n",InitBuc.c_str(),perfs[InitBuc]);
        printf("%s cost:%lf\n",EditDir.c_str(),perfs[EditDir]);
        printf("%s cost:%lf\n",FreeLock.c_str(),perfs[FreeLock]);
        printf("%s cost:%lf\n",ReadOldBuc.c_str(),perfs[ReadOldBuc]);
        printf("%s cost:%lf\n",ReadKv.c_str(),perfs[ReadKv]);
        printf("%s cost:%lf\n",MoveSlot.c_str(),perfs[MoveSlot]);
        printf("%s cost:%lf\n",UpdateDep.c_str(),perfs[UpdateDep]);
        printf("%s cost:%lf\n",CasSlot.c_str(),perfs[CasSlot]);
        printf("%s cost:%lf\n",RRead.c_str(),perfs[RRead]);
        printf("%s cost:%lf\n",CheckDuplicate.c_str(),perfs[CheckDuplicate]);
        printf("%s cost:%lf\n",IsCorrectBucket.c_str(),perfs[IsCorrectBucket]);   
        printf("%s cnt:%lu\n",SplitCnt.c_str(),cnts[SplitCnt]);   
        printf("%s cnt:%lu\n",SlotCnt.c_str(),cnts[SlotCnt]);   
#endif
    }

};