#ifndef EBR_H
#define EBR_H

#include <stddef.h>
#include <stdbool.h>

#ifndef NODE_TYPE
    #define NODE_TYPE item
#endif

#include "bag.h"


typedef struct ebr ebr;
//Global ebr struct
struct ebr {
    volatile uint64_t curr_epoch;
    uint64_t *announcements;
    int num_threads;
    bool *quiescent_bits;
    void (*reclaim)(void*);
};

typedef struct reclamation reclamation;
//Thread's view of ebr
struct reclamation {
    ebr *r;
    uint64_t *announcement;
    volatile bool *quiescent_bit;
    bag** limbo_bags;
    bag* to_be_reclaimed; /* Reclaimed items */
    uint32_t total_reclaimed;
};


ebr* init_ebr(int num_threads, void (*reclaim)(void*));
void free_ebr(ebr* r);
reclamation* init_reclamation(ebr* r, int tid, size_t bag_sizes);
void free_reclamation(reclamation* recl);
void announce_epoch(reclamation* recl);
int can_advance_epoch(ebr* r);
int try_advance_epoch(ebr* r);
void add_retired_item(reclamation* recl, NODE_TYPE* item, int reclaim_type);
int transfer_retired(reclamation* recl);
NODE_TYPE* remove_retired_item(reclamation* recl);
void empty_oldest_limbo(reclamation* recl, unsigned long epoch_to_empty);
NODE_TYPE* get_safe_to_reclaim(reclamation* recl);
void reclaim(reclamation *recl);

void enter_quiescent(reclamation *recl);
void leave_quiescent(reclamation *recl);
bool is_quiescent(reclamation *recl);

void print_info(ebr* r, reclamation* recl);



//TODO:
//	this was a quick workaround for the assoc maintenance thread
//	to be able to reclaim items both to the OS and the slab allocator
//	at will. It is probably too confusing and unnecessarily unsafe.
//	Could be implemented much better
#define Type uintptr_t

//Custom reclaim function given in the beginning
#define CUSTOM_RECLAIM		recl->r->reclaim
#define CUSTOM_TYPE			1

//Reclaim to Operating System (normal free)
#define OS_RECLAIM			&free
#define OS_TYPE				2

#define get_os_marked_reference(x)   	((Type) x | 1)		//Marks reference as to be reclaimed to OS
#define is_os_marked_reference(x) 		((Type) x & 1)		//Checks if references should be reclaimed to OS
#define get_unmarked_reference(x) 	((Type) x & ~(Type) 3)	//Unmarks references
															
															
#endif
