/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <signal.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "nblist.h"

/* how many powers of 2's worth of buckets we use */
unsigned int hashpower = HASHPOWER_DEFAULT;

//Amount of collision lists
#define hashsize(n) ((uint64_t)1<<(n))

//Masks to know to what collision list an item goes to
#define hashmask(n) (hashsize(n)-1)

/* Main hash table. This is where we look except during expansion. */
static List** hashtable = 0;


/* Clock related */
#define JOIN(a,b,c) a ## b ## c
#define GET_CLOCK_TYPE(size) JOIN(uint, size, _t)
#define GET_CLOCK_MAX(size) JOIN(UINT, size, _MAX)

#define CLOCK_SIZE 8 //Size of each clock variable in bits
#define CLOCK_TYPE GET_CLOCK_TYPE(CLOCK_SIZE)

//#define CLOCK
#ifdef CLOCK
    #define CLOCK_MAX 1
    #define INC_FACTOR 1
    #define DEC_FACTOR 1
#else
    #define CLOCK_MAX GET_CLOCK_MAX(CLOCK_SIZE)
    #define INC_FACTOR 1
    #define DEC_FACTOR 1
#endif

static CLOCK_TYPE* clock_val = NULL;
static __thread uint32_t hand = 0;

//Array with number of current items
//  cannot be uint because one thread might add and other remove
static int32_t* curr_items = NULL; 

void assoc_init(const int hashtable_init) {
    if (hashtable_init) {
        hashpower = hashtable_init;
    }
    hashpower = 13;

    //Allocate space for hashsize lists
    hashtable = calloc(hashsize(hashpower), sizeof(List *));
    if (!hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }

    //Initialize a list for each hashtable collision list
    for (int i = 0; i < hashsize(hashpower); ++i)
        hashtable[i] = new_nblist();

    clock_val = calloc(hashsize(hashpower), sizeof(CLOCK_TYPE));
    if (!clock_val) {
        fprintf(stderr, "Failed to init CLOCK values.\n");
        exit(EXIT_FAILURE);
    }

    //Allocate array that keeps track of total number of items
    curr_items = calloc(settings.num_threads, sizeof(int32_t));

    STATS_LOCK();
    stats_state.hash_power_level = hashpower;
    stats_state.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

void static inline inc_clock(uint32_t bucket) {
    CLOCK_TYPE *clock = &clock_val[bucket];
    CLOCK_TYPE val = *clock;
    if(val < CLOCK_MAX - INC_FACTOR) {
        *clock = val + INC_FACTOR;
    } else {
        *clock = CLOCK_MAX;
    }
}

uint32_t static inline dec_clock(uint32_t bucket) {
    CLOCK_TYPE *clock = &clock_val[bucket];
    CLOCK_TYPE val = *clock;
    if(val > DEC_FACTOR) {
        *clock = val - DEC_FACTOR;
        return val;
    } else {
        *clock = 0;
        return val;
    }
}

item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    List *l;
    uint32_t hmask;

    hmask = hv & hashmask(hashpower);
    inc_clock(hmask);

    l = hashtable[hmask];
    it = get(l, key, nkey);

    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return it;
}

int assoc_insert(item *it, const uint32_t hv) {
    List *l;
    uint32_t hmask;
    int ret;

    hmask = hv & hashmask(hashpower);
    inc_clock(hmask);

    l = hashtable[hmask];

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey);

    ret = insert(l, it);
    if(ret) {
        curr_items[tid]++;
    }

    return ret;
}

int assoc_delete(const char *key, const size_t nkey, const uint32_t hv) {
    List *l;
    uint32_t hmask;
    int ret;

    hmask = hv & hashmask(hashpower);

    //Should decrement CLOCK reference?
    l = hashtable[hmask];
    
    ret = del(l, key, nkey) != NULL;
    if(ret) {
        curr_items[tid]--;
    }

    return ret;
}

void assoc_bump(item *it, const uint32_t hv) {
    uint32_t hmask;
    hmask = hv & hashmask(hashpower);
    inc_clock(hmask);
    //assoc_delete(ITEM_key(it), it->nkey, hv);
    //assoc_insert(it, hv);    
}


/* Returns number of items removed. */
int try_evict(const int orig_id, const uint64_t total_bytes, const rel_time_t max_age) {
    List *l;

    //Slab where we wanted to alloc (and therefore called this function)
    int id = orig_id; 
    if (id == 0)
        return 0;

    size_t removed = 0;

    size_t num_buckets = hashsize(hashpower);

    uint32_t c = 0;
    while(c++ < num_buckets) { //Do one trip around every bucket at maximum
        hand = (hand + 1) % num_buckets;

        //Update this bucket's clock val
        if(dec_clock(hand) == 0) {
            l = hashtable[hand];

            //Only try to evict non-empty buckets
            //if(is_empty(l)) { continue; }

            //Try to pop off bucket head
            removed = empty_list(l);

            //If no one removed list might of been empty or removal failed
            if(removed > 0) {
                curr_items[tid] -= removed; 
                return removed;
            }
        }
    }

    return removed;
}

int32_t get_curr_items() {
    int32_t res = 0;
    for(int i = 0; i < settings.num_threads; i++)
        res += curr_items[i];
    return res;
}
