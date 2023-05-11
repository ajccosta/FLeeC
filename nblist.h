#ifndef NBLIST_H
#define NBLIST_H

#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include "memcached.h"
#include "ebr.h"

//Assumes that insert by index wont be used:
#define KEY_cmp(key1, key2, size1, size2) __extension__({int32_t diff = (size1 - size2); \
    diff != 0 ? diff : \
    (strncmp(key1, key2, size1));})

#define ITEM_cmp(it1, it2) KEY_cmp(ITEM_key(it1), ITEM_key(it2), it1->nkey, it2->nkey)

/* Declarations */
typedef struct List {
	item *head;
	item *tail;
} List;


void free_list(List* l);
void print_list(List * list);

bool check_alignment(void);

List* new_nblist(void);
int cleanup(List* list);
int empty_list(List* list);
bool is_empty(List *list);
int __mark_all_nodes(List* list);
item* search(List* list, const char* search_key, const size_t nkey, item **left_item);
bool insert(List *list, item *it);
item* del(List* list, const char* search_key, const size_t nkey, bool reclaim, bool *found);
bool find(List *list, const char* search_key, const size_t nkey);
item* get(List *list, const char* search_key, const size_t nkey);
item* search_index(List* list, const int index, item **left_item, bool is_delete);
item* get_index(List *list, const int index);
bool insert_index(List *list, item* it, int index);
item* del_index(List* list, const int index);
bool insert_head(List *list, item* it);
bool insert_tail(List *list, item* it);
item* del_head(List* list);
item* del_tail(List* list);

typedef struct { //TODO: Check if it is aligned correctly with memcached's item
    struct _stritem *next;
} fake_item;


/*---------------------------DECLARATION---------------------------*/
/* Reclamation related variables */
extern __thread reclamation* recl;

void static inline ebr_add_retired_item(item* item) {add_retired_item(recl, item);}
void static inline ebr_announce_epoch() {announce_epoch(recl);}
void static inline ebr_enter_quiescent() {enter_quiescent(recl);}
void static inline ebr_leave_quiescent() {leave_quiescent(recl);}


//"Generic" key type (equivalent to void)
#define KeyType uintptr_t
/* Marking references */
#define get_marked_reference(x)   	((KeyType) x | 1)
#define get_unmarked_reference(x) 	((KeyType) x & ~(KeyType) 1)
#define is_marked_reference(x) 		((KeyType) x & 1)

#endif
