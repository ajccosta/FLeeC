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
volatile unsigned int hashpower = HASHPOWER_DEFAULT;

//Amount of collision lists
#define hashsize(n) ((uint64_t)1<<(n))

//Masks to know to what collision list an item goes to
#define hashmask(n) (hashsize(n)-1)

/* Main hash table. This is where we look except during expansion. */
static List** hashtable = 0;
static List** new_hashtable = 0; /* hash table that is being expanded into */


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
static CLOCK_TYPE* new_clock_val = NULL;
static __thread uint32_t hand = 0;

//Array with number of current items
//  cannot be uint because one thread might add and other remove
static int64_t* curr_items = NULL; 

/* Maintenence thread  / expansion */
static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t maintenance_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile bool expanding = false;

void assoc_init(const int hashtable_init) {
    if (hashtable_init) {
        hashpower = hashtable_init;
    }

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
    curr_items = calloc(settings.num_threads, sizeof(int64_t));

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

    if(expanding && it == NULL) {
        //Look in other bucket as well, if we are expanding
        //  and item was not in the first bucket
        hmask = hv & hashmask(hashpower);
        inc_clock(hmask);
        l = new_hashtable[hmask];
        it = get(l, key, nkey);
    }

    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return it;
}

int assoc_insert(item *it, const uint32_t hv) {
    List *l;
    uint32_t hmask;
    int ret;

    if(expanding) {
        hmask = hv & hashmask(hashpower + 1);
        l = new_hashtable[hmask];
        //Dont change CLOCK while expanding
    } else {
        hmask = hv & hashmask(hashpower);
        l = hashtable[hmask];
        inc_clock(hmask);
    }


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
    
    bool found = false;
    ret = del(l, key, nkey, true, &found) != NULL;

    if(ret || found) {
        curr_items[tid]--;

    } else if(expanding) {
        //Also look in other bucket,
        //  item was not found in first
        hmask = hv & hashmask(hashpower + 1);
        l = new_hashtable[hmask];
        ret = del(l, key, nkey, true, &found) != NULL;

        if(ret || found) {
            curr_items[tid]--;
        }
    }

    return ret;
}

int assoc_replace(item *old_it, item *new_it, const uint32_t hv) {
    List *l;
    uint32_t hmask;
    bool inserted;

    if(expanding) {
        //TODO: Think about how expansion could mess this up!
        hmask = hv & hashmask(hashpower + 1);
        l = new_hashtable[hmask];
        //Dont change CLOCK while expanding

    } else {
        hmask = hv & hashmask(hashpower);
        l = hashtable[hmask];
        inc_clock(hmask);
    }

retry:
    replace(l, ITEM_key(old_it), old_it->nkey, new_it, true, &inserted);
    if(!inserted) {
	goto retry;
    }

    return inserted;
}

void assoc_bump(item *it, const uint32_t hv) {
    uint32_t hmask;
    hmask = hv & hashmask(hashpower);
    inc_clock(hmask);

    //Dont change CLOCK while expanding

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

uint64_t get_curr_items() {
    int64_t res = 0;
    for(int i = 0; i < settings.num_threads; i++)
        res += curr_items[i];
    return (uint64_t) res;
}

int start_assoc_maintenance_thread(ebr *r) {
    int ret;
    pthread_t thread;

    if((ret = pthread_create(&thread, NULL, assoc_maintenance_thread, (void*) r)) != 0) {
        fprintf(stderr, "Failed to start maintenance thread: %s\n", strerror(ret));
        return -1;
    }
    
    return 0;
}

/* Check if we should expand hash table */
void assoc_check_expand() {
    if (pthread_mutex_trylock(&maintenance_lock) == 0) {
        uint64_t curr = get_curr_items();

        /* If there are 1.5 more items than there are buckets, expand */
        if (curr > (hashsize(hashpower) * 3) / 2 && hashpower < HASHPOWER_MAX) {
            pthread_cond_signal(&maintenance_cond);
        }
        pthread_mutex_unlock(&maintenance_lock);
    }
}

void start_expansion() {
    unsigned int old_hashpower = hashpower;
    unsigned int new_hashpower = old_hashpower + 1;

    new_clock_val = calloc(hashsize(new_hashpower), sizeof(CLOCK_TYPE));
    if (new_clock_val) {
        //Copy CLOCK values to new buckets
        for (unsigned int i = hashsize(hashpower); i < hashsize(new_hashpower); ++i) {
            unsigned int clock_counterpart = i - hashsize(hashpower);
            new_clock_val[i] = clock_val[clock_counterpart];
        }

    } else {
        return;
    }

    //Allocate space for hashsize lists
    new_hashtable = calloc(hashsize(new_hashpower), sizeof(List *));
    if (new_hashtable) {

        //Transfer existing buckets to new hashtable
        for (unsigned int i = 0; i < hashsize(old_hashpower); ++i)
            new_hashtable[i] = hashtable[i];

        //Create new buckets
        for (unsigned int i = hashsize(old_hashpower); i < hashsize(new_hashpower); ++i)
            new_hashtable[i] = new_nblist();

        //Threads can now insert into new hash table
        //If we incremented hashpower here, then there might be threads
        //  that see that the table is expanding but do not see new hashpower
        //  (because those operations are not atomic). So, threads should calculate
        //  the new hashpower themselves if they observe that an expansion is occurring
        expanding = true;

        //TODO: lookups and deletes must be done in both "hashpowers" during expansion
    
        if(settings.verbose > 0)
            fprintf(stderr, "Starting expansion from %d to %d\n", hashpower, hashpower + 1);
    } else {
        free(new_clock_val);
    }
}


#define ASSOC_MAINTENENCE_THREAD_SLEEP 10000

void *assoc_maintenance_thread(void *arg) {
    tid = settings.num_threads;
    ebr *r = (ebr*) arg; /* Main ebr struct */
    reclamation *recl = init_reclamation(r, settings.num_threads, 1);
    enter_quiescent(recl);

    //Required for cond signal to work (and unlock this)
    mutex_lock(&maintenance_lock);
    bool expanded_last_iter = false;

    while(true) {

        //Do not expand twice in a row (without waiting for cond)
        if(expanding && !expanded_last_iter) {
            expanded_last_iter = true;

            //Wait for two epochs, so that no thread thinks the
            //  old hashtable is the current hashtable and inserts
            //  new items there
            uint64_t curr_epoch = r->curr_epoch;
            while(r->curr_epoch < curr_epoch + 2) {
                announce_epoch(recl);
                enter_quiescent(recl);
                usleep(ASSOC_MAINTENENCE_THREAD_SLEEP);
            }

            int old_hashpower = hashpower;
            int old_hashsize = hashsize(old_hashpower);

            leave_quiescent(recl);

            for(uint32_t i = 0; i < old_hashsize; ++i) {
                item *head, *tail, *it, *next;

                List *l = hashtable[i]; //old hash table
                head = l->head;
                tail = l->tail;
                
                //Traverse items in bucket
                for(it = head->next; it != tail; it = next) {
                    char *key = ITEM_key(it);
                    uint8_t size = it->nkey;
                    uint32_t item_hash = hash(key, size);
                    uint32_t new_bucket = item_hash & hashmask(old_hashpower + 1);

                    //Read next now because if we reinser it will change
                    next = (item*) get_unmarked_reference(it->next);

                    if(i != new_bucket) {
                        //hash mask's left most bit is not 0, change item's bucket

                        List *new_list = new_hashtable[new_bucket];

                        //During this time, the item is not visible
                        //There is also the chance that an item is marked and deleted
                        //  by anoter thread, deleting it permanently
                        //TODO: maybe mark the 2nd least significant bit as well
                        //  to prevent this from happening?

                        bool unused, ret;
                        if(del(l, key, size, false, &unused)) { //TODO: This can be optimized, we already searched
                            ret = insert(new_list, it);

							if(!ret) {
								//Item not inserted for whatever reason,
								//	should be safe to reclaim as it is 
								//	not accessible through the data structure.
								
								//TODO: test this (?)
       							add_retired_item(recl, it, CUSTOM_TYPE);
 
                            	if(settings.verbose > 0) {
                            	    printf("item %.*s, was already in %d\n",
                            	        it->nkey, ITEM_key(it), new_bucket);
                            	}


							} else if(settings.verbose > 1) {
                                printf("Replaced item %.*s, from bucket %d to %d\n",
                                    it->nkey, ITEM_key(it), i, new_bucket);
                            }
                        }
                    }
                }

            }

            //Finish expanding
            //TODO: is this safe? The OS should not care
            add_retired_item(recl, (void*) hashtable, OS_TYPE);
            add_retired_item(recl, (void*) clock_val, OS_TYPE);

            hashtable = new_hashtable;
            clock_val = new_clock_val;

			//Try and advance 2 epochs again, so that 
			//	we reclaim the hash table, the clock values
			//	and any items that we might of retired
			//	during the hash table process
			curr_epoch = r->curr_epoch;
            while(r->curr_epoch < curr_epoch + 2) {
                announce_epoch(recl);
                enter_quiescent(recl);
                usleep(ASSOC_MAINTENENCE_THREAD_SLEEP);
            }

            enter_quiescent(recl);

            expanding = false;
            hashpower++;

            if(settings.verbose > 0) {
                fprintf(stderr, "Expansion ended\n");
            }

        } else {
            expanded_last_iter = false;
            pthread_cond_wait(&maintenance_cond, &maintenance_lock);
            start_expansion();
        }
    }

    mutex_unlock(&maintenance_lock);
    return NULL;
}
