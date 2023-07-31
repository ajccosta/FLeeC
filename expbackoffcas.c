#include "expbackoffcas.h"
#include <stdbool.h>

#define MIN(x, y) (((x) < (y)) ? (x) : (y))

#define EXP_THRESHOLD 1
#define C 15
#define M 10


int exp_threshold = EXP_THRESHOLD;
int c = C;
int m = M;

static __thread uint32_t failures = 0;

__thread uint32_t cas_hits = 0;
__thread uint32_t cas_misses = 0;

#ifdef BACKOFF
int ExpBackoffCAS(volatile uintptr_t *p, uintptr_t  *e, uintptr_t d) {
    if(_CAS(p, e, d)) {
        if(failures > 0) {
            failures--;
        }

        ++cas_hits;
        return true;

    } else {
    	if(++failures > exp_threshold) {
            int min = MIN(c * failures, m);
            usleep(min*min);
        }

        ++cas_misses;
        return false;
    }
}

#else

//Count failures only, no backoff
int ExpBackoffCAS(volatile uintptr_t *p, uintptr_t  *e, uintptr_t d) {
	if(_CAS(p, e, d)) {
        ++cas_hits;
		return true;
	} else {
        ++cas_misses;
	    return false;
	}
}
#endif
