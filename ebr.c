/*
 * Although the file name is ebr.c, this is actually closer
 * to debra than ebr as it is a mixture of ebr and qsbr
 */

#include "memcached.h"
#ifndef NODE_TYPE
    #define NODE_TYPE item
#endif
#include "ebr.h"
//#include "expbackoffcas.h"

#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <stdatomic.h>

//These two are the same thing
//#define CAS(p, e, d) __atomic_compare_exchange_n(p, e, d, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED)
#define CAS(p, e, d) atomic_compare_exchange_weak(p, e, d)

//Initialize global structure that coordinates epochs
ebr* init_ebr(int num_threads, void (*reclaim)(void*)) {
    ebr* r = (ebr*) malloc(sizeof(ebr));
    r->curr_epoch = 1;
    r->announcements = calloc(num_threads, sizeof(uint64_t));
    r->quiescent_bits = calloc(num_threads, sizeof(bool));
    r->num_threads = num_threads;
    r->reclaim = reclaim;
    return r;
}

//Free ebr structure
void free_ebr(ebr* r) {
    free(r->announcements);
    free(r->quiescent_bits);
    free(r);
}

//Initialize thread local structure that coordinates epochs from the view of each thread
reclamation* init_reclamation(ebr* r, int tid, size_t bag_sizes) {
    reclamation* recl = (reclamation*) malloc(sizeof(reclamation));
    recl->r = r;
    recl->announcement = &(r->announcements[tid]);
    recl->quiescent_bit = &(r->quiescent_bits[tid]);
    *(recl->quiescent_bit) = true;

    recl->limbo_bags = (bag**) malloc(3 * sizeof(bag*));
    for(int i = 0; i < 3; ++i)
        recl->limbo_bags[i] = create_bag(bag_sizes);

    recl->to_be_reclaimed = create_bag(bag_sizes);

    return recl;
}

//Free reclamation structure and items it contains
void free_reclamation(reclamation* recl) {
    NODE_TYPE* item;
    void (*reclaim_func)(void*) = recl->r->reclaim;
    //Free remaining items in limbo bags
    for(int i = 0; i < 3; i++) {
        bag* curr_bag = recl->limbo_bags[i];
        while((item = take(curr_bag)) != NULL) { 
            (*reclaim_func)(item);
        }
    }

    //Free remaining items in to_be_reclaimed bag
    while((item = take(recl->to_be_reclaimed)) != NULL) { 
        (*reclaim_func)(item);
    }

    for(int i = 0; i < 3; i++)
        free_bag(recl->limbo_bags[i]);
    free(recl->limbo_bags);

    free_bag(recl->to_be_reclaimed);

    free(recl);
}

//To call when entering data structure
//EBR assumes that when this is called no non-retired object's pointer is held
void announce_epoch(reclamation* recl) {
    uint64_t curr_epoch = recl->r->curr_epoch;
    assert(*recl->announcement <= curr_epoch);

    leave_quiescent(recl);

    if(curr_epoch >= 2)
        empty_oldest_limbo(recl, curr_epoch - 2);

    if(curr_epoch > *recl->announcement) {
        //Updated threads' epoch by announcing the current epoch
        (*recl->announcement) = curr_epoch;
        reclaim(recl); //Epoch was advanced, can reclaim e-2
    }

    if(try_advance_epoch(recl->r))
        reclaim(recl); //Epoch was advanced, can reclaim e-2
}

//Try to atomically increment the current epoch
//  returns wether or not the epoch was incremented
int try_advance_epoch(ebr* r) {
    uint64_t c;
    if((c = can_advance_epoch(r))) {
        return CAS(&r->curr_epoch, &c, c + 1);
    }
    return 0;
}

//Check if the epoch can has been announced by all threads
int can_advance_epoch(ebr* r) {
    int n = r->num_threads;
    int curr_epoch = r->curr_epoch;
    for(int i = 0; i < n; ++i) {
        //This is safe because curr_epoch is a local variable.
        //Even if r->curr_epoch is updated concurrently,
        //the result is still correct
        if(r->announcements[i] < curr_epoch && !r->quiescent_bits[i]) {
            return 0; /* Can not advance current epoch */
        }
    }
    return curr_epoch; /* Can try to advance from curr_epoch to curr_epoch + 1 */
}



//Add a item to the retired items set
void add_retired_item(reclamation* recl, NODE_TYPE* item, int reclaim_type) {
	void* marked_item = NULL;

    uint64_t epoch = recl->r->curr_epoch;
    uint8_t curr_bag_index = epoch % 3;
    bag* curr_bag = recl->limbo_bags[curr_bag_index];


	switch(reclaim_type) {
		case CUSTOM_TYPE:
			marked_item = (void*) item; //most common, do not mark
			break;
		case OS_TYPE:
			marked_item = (void*) get_os_marked_reference(item); //most common, do not mark
			break;
	}

    /* insert item into limbo bag */
    put(curr_bag, marked_item);
}
 
//Transfers limbo bag into the to_be_reclaimed bag
//<epoch_to_empty> must always be e-2 for safe behavior
void empty_oldest_limbo(reclamation* recl, uint64_t epoch_to_empty) {
    //limbo bag of epoch e-2
    uint8_t curr_bag_index = epoch_to_empty % 3;
    bag* curr_bag = recl->limbo_bags[curr_bag_index];
    
    transfer(recl->to_be_reclaimed, curr_bag);
}

//Returns one item that is safe to reclaim
NODE_TYPE* get_safe_to_reclaim(reclamation* recl) {
    return take(recl->to_be_reclaimed);
}

//Reclaims all items that are safe to reclaim
void reclaim(reclamation *recl) {
    NODE_TYPE *n;
    void (*reclaim_func)(void*);

    while((n = get_safe_to_reclaim(recl)) != NULL) {
		if(is_os_marked_reference(n)) {
			//Reclaim by to OS
			reclaim_func = OS_RECLAIM;
			n = (void*) get_unmarked_reference(n);

		} else {
			//Reclaim to custom reclaimer function
			reclaim_func = CUSTOM_RECLAIM;
		}

        (*reclaim_func)(n);
        recl->total_reclaimed++;
    }
}

//"Stop messing" with the data-structure
void enter_quiescent(reclamation *recl) {
    *(recl->quiescent_bit) = true;

	//Advance epoch so that threads that are
	//	activelly trying to reclaim can do so
    uint64_t curr_epoch = recl->r->curr_epoch;
    if(curr_epoch > *recl->announcement) {
        //Updated threads' epoch by announcing the current epoch
        (*recl->announcement) = curr_epoch;
        reclaim(recl); //Epoch was advanced, can reclaim e-2
    }
}

//"Start messing" with the data-structure
void leave_quiescent(reclamation *recl) {
    *(recl->quiescent_bit) = false;
}


bool is_quiescent(reclamation *recl) {
    return *(recl->quiescent_bit);
}

void print_info(ebr* r, reclamation* recl) {
    printf("epoch: %ld; ", r->curr_epoch);

    printf("announcs: ");
    for(int i = 0; i < r->num_threads; i++)
        printf("%ld ", r->announcements[i]);

    if(recl != NULL) {
        printf("reclaimed: %d ", recl->to_be_reclaimed->curr_in_bag);
        int i = 0;
        for(; i < 2; i++)
            printf("bag[%d]:%d, ", i, recl->limbo_bags[i]->curr_in_bag);
        printf("bag[%d]:%d", i, recl->limbo_bags[i]->curr_in_bag);
    }
    printf("\n");
}
