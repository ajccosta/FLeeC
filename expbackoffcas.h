#ifndef EXPBACKOFFCAS_H
#define EXPBACKOFFCAS_H

#include <stdint.h>
#include <unistd.h>
#include <stdatomic.h>
#include <stdint.h>


//Substitute builtin CAS by function
#ifdef CAS 
    #undef CAS
#endif

#define _CAS(p, e, d) __atomic_compare_exchange_n(p, e, d, \
    0, __ATOMIC_RELAXED, __ATOMIC_RELAXED)



//ifdef BACKOFF, then CAS backs off; else only count failures
#define BACKOFF


#define CAS(p, e, d) ExpBackoffCAS((uintptr_t*)(p), (uintptr_t*)(e), (uintptr_t)(d))

int ExpBackoffCAS(volatile uintptr_t *p, uintptr_t  *e, uintptr_t d);

#endif
