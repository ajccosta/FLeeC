#include "nblist.h"
#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include <stdbool.h>
#include <string.h>


/* Compare and Swap macro */
#define CAS(p, e, d) __atomic_compare_exchange_n(p, e, d, \
    0, __ATOMIC_RELAXED, __ATOMIC_RELAXED)


//this include redefiens CAS,
//	so it should be included after
//	#define CAS
//#include "expbackoffcas.h"



//Must only be called sequentially
void free_list(List* l) {
    item *n = l->head->next, *next = NULL;
    while(n != l->tail && n != NULL) {
        next = (item*) get_unmarked_reference(n->next);
        free(n);
        n = next;
    }
    free(l->head);
    free(l->tail);
    free(l);
}

void print_list(List * list) {
    item* n = list->head;
    item* next = list->head->next;
    item* keep_real = next;
    int c = 0;
    while(next != list->tail) {
        n = next;
        keep_real = n->next;
        //printf("%.*s:%ld:%d:%d:%d ",
        //    n->nkey, ITEM_key(n), is_marked_reference(keep_real),
        //    is_marked_replacement_reference(keep_real) > 0, n->id,
        //    n->tid);
        printf("%.*s:%ld:%d ",
            n->nkey, ITEM_key(n), is_marked_reference(keep_real),
            is_marked_replacement_reference(keep_real) > 0);
        c++;
        next = (item *) get_unmarked_reference(n->next);
    }
    printf("\n");
}


//Check if alignement of these 2 elements in 2 structs is the same
bool check_alignment() {
    item* it = (item*) malloc(sizeof(item));
    fake_item* fake_it = (fake_item*) malloc(sizeof(fake_item));
    uint8_t next_alignment = (long) &(it->next) - (long) it; 
    uint8_t fake_next_alignment = (long) &(fake_it->next) - (long) fake_it; 
    free(it);
    free(fake_it);
    return (next_alignment == fake_next_alignment);
}



List* new_nblist(void) {
    if(!check_alignment()) {
        fprintf(stderr, "Alignment of struct item and struct fake_item differs!");
        exit(EXIT_FAILURE);
    }
	List *l = (List*) malloc(sizeof(List));
	fake_item *head = (fake_item*) malloc(sizeof(fake_item));
	fake_item *tail = (fake_item*) malloc(sizeof(fake_item));
	l->head = (item*) head;
	l->tail = (item*) tail;
	head->next = l->tail;
	return l;
}





#ifdef MARK_REPLACEMENT //MARK REPLACEMENT------------------------------------------

#define MAX_REPLACE_RETRIES 5000

item* search(List* list, const char* search_key, const size_t nkey, item **left_item,
    bool ignore_replacement) {

	//NULL because of warnings
	item *left_item_next = NULL, *right_item = NULL;

    int replace_retries = 0; //Number of times retried because of replace marked items

search_again:

    if(!ignore_replacement && right_item != NULL &&
        is_marked_replacement_reference(right_item->next)) {

        //We retried because of replace marking
        replace_retries++;

        if(replace_retries >= MAX_REPLACE_RETRIES) {
            //Thread replacing has likely crashed
            //  try and finish part of the job, i.e., delete old item
            del_by_ref(list, right_item, true);
            return NULL; //Did not find item, abort
        }
    }

	do {
        item *t = list->head;
        item *t_next = list->head->next; 
        int marked_counter = 0;

		/* 1: Find left_item and right_item */
        do {
            if (!is_marked_reference(t_next)) {
                (* left_item) = t;
                left_item_next = t_next;
                marked_counter = 0;
            } else {
                marked_counter++;
            }

            t = (item *) get_unmarked_reference(t_next);
            if (t == list->tail)
				break;
            t_next = t->next;
        } while (is_marked_reference(t_next) ||
            //Compare keys
            (KEY_cmp(ITEM_key(t), search_key, t->nkey, nkey) < 0)); /*B1*/

        right_item = t; 
		/* 2: Check items are adjacent */
        if (left_item_next == right_item) {

            if ((right_item != list->tail) &&
                (is_marked_reference(right_item->next) ||
                (!ignore_replacement && is_marked_replacement_reference(right_item->next))))
                goto search_again; /*G1*/
			else
				return right_item; /*R1*/
		}

 		/* 3: Remove one or more marked items */
        if (CAS(&((*left_item)->next), &left_item_next, right_item)) { /*C1*/
            //Add one or more marked items to be reclaimed
            item *e = (item*) get_unmarked_reference(left_item_next);
            while(e != NULL && marked_counter > 0) {
                ebr_add_retired_item(e, CUSTOM_TYPE);
                assert(is_marked_reference(e->next));
                e = (item*) get_unmarked_reference(e->next);
                marked_counter--;
            }

            if ((right_item != list->tail) &&
                (is_marked_reference(right_item->next) ||
                (!ignore_replacement && is_marked_replacement_reference(right_item->next))))
				goto search_again; /*G2*/
            else
		      	return right_item; /*R2*/
		}

    } while (true); /*B2*/
}
//------------------------------------------MARK REPLACEMENT


#else


//POSTERIOR INSERTION REPLACEMENT---------------------------
item* search(List* list, const char* search_key, const size_t nkey, item **left_item) {
	//NULL because of warnings
	item *left_item_next = NULL, *right_item;

search_again:
	do {
        item *t = list->head;
        item *t_next = list->head->next; 
        int marked_counter = 0;

		/* 1: Find left_item and right_item */
        do {
            if (!is_marked_reference(t_next)) {
                (* left_item) = t;
                left_item_next = t_next;
                marked_counter = 0;
            } else {
                marked_counter++;
            }

            t = (item *) get_unmarked_reference(t_next);
            if (t == list->tail)
				break;
            t_next = t->next;
        } while (is_marked_reference(t_next) ||
            //Compare keys
            (KEY_cmp(ITEM_key(t), search_key, t->nkey, nkey) < 0)); /*B1*/

        right_item = t; 
		/* 2: Check items are adjacent */
        if (left_item_next == right_item) {
            if ((right_item != list->tail) && is_marked_reference(right_item->next))
                goto search_again; /*G1*/
			else
				return right_item; /*R1*/
		}

 		/* 3: Remove one or more marked items */
        if (CAS(&((*left_item)->next), &left_item_next, right_item)) { /*C1*/
            //Add one or more marked items to be reclaimed
            item *e = (item*) get_unmarked_reference(left_item_next);
            while(e != NULL && marked_counter > 0) {
                ebr_add_retired_item(e, CUSTOM_TYPE);
                assert(is_marked_reference(e->next));
                e = (item*) get_unmarked_reference(e->next);
                marked_counter--;
            }

            if ((right_item != list->tail) && is_marked_reference(right_item->next))
				goto search_again; /*G2*/
            else
		      	return right_item; /*R2*/
		}

    } while (true); /*B2*/
}
//---------------------------POSTERIOR INSERTION REPLACEMENT
#endif 




int cleanup(List* list) {
	item *left_item_next, *right_item, *left_item;
	left_item_next = right_item = left_item = NULL;
    int items_removed;
    int total_items_removed = 0;

    do {
        item * t = list->head;
        item * t_next = list->head->next; 

        items_removed = 0;
continue_cleanup:
        /* Traverse until we find a marked item */
        do {
            if (!is_marked_reference(t_next)) {
                left_item = t;
                left_item_next = t_next;
            }
            t = (item *) get_unmarked_reference(t_next);
            if (t == list->tail)
				return total_items_removed; /* Did not find marked items */
            t_next = t->next;
        } while (!is_marked_reference(t_next));

		/* 1: Find left_item and right_item */
        while (is_marked_reference(t_next)) {
            items_removed++;
            t = (item *) get_unmarked_reference(t_next);
            if (t == list->tail)
				break;
            t_next = t->next;
        }

        right_item = t; 

 		/* 3: Remove one or more marked items */
        if (CAS(&(left_item->next), &left_item_next, right_item)) {
            item *e = (item*) get_unmarked_reference(left_item_next);
            total_items_removed += items_removed;
            while(e != NULL && items_removed > 0) {
                ebr_add_retired_item(e, CUSTOM_TYPE);
                assert(is_marked_reference(e->next));
                e = (item*) get_unmarked_reference(e->next);
                items_removed--;
            }
            goto continue_cleanup;
		} /* else { retry; }*/

    } while (true); /*B2*/
}

//Mark every node in list as logically deleted
int __mark_all_nodes(List* list) {
    item *tail, *e, *e_next;
    e = list->head->next;
    tail = list->tail;
    int marked_nodes = 0;

	while (e != tail) {
        do {
            e_next = e->next;

            if (is_marked_reference(e_next) ||
                CAS(&(e->next), &e_next, (item*) get_marked_reference(e_next)))
		    		break;

        } while(true);

        marked_nodes++;
        e = (item*) get_unmarked_reference(e->next);
    }

    return marked_nodes;
}

//Wrapper for mix of routines that empty a list
int empty_list(List* list) {
    __mark_all_nodes(list);
    return cleanup(list);
}

bool is_empty(List *list) {
    return list->head->next == list->tail;
}

bool insert(List *list, item *it) {
    item *right_item, *left_item;

    do {
#ifdef MARK_REPLACEMENT
        right_item = search(list, ITEM_key(it), it->nkey, &left_item, false);
#else
        right_item = search(list, ITEM_key(it), it->nkey, &left_item);
#endif

        if ((right_item == NULL) ||
            ((right_item != list->tail) && (ITEM_cmp(right_item, it) == 0))) /*T1*/
			return false;

        it->next = right_item;

        if (CAS(&(left_item->next), &right_item, it)) /*C2*/
            return true;

    } while (true); /*B3*/
}

item* del(List* list, const char* search_key, const size_t nkey, bool reclaim, bool *found) {
    item *right_item, *right_item_next, *left_item = NULL;

    do {
#ifdef MARK_REPLACEMENT
        right_item = search(list, search_key, nkey, &left_item, false);
#else
        right_item = search(list, search_key, nkey, &left_item);
#endif
        if ((right_item == NULL) || (right_item == list->tail) ||
            (KEY_cmp(ITEM_key(right_item), search_key, right_item->nkey, nkey) != 0)) /*T1*/
            return NULL;

        right_item_next = right_item->next;

        if (!is_marked_reference(right_item_next))
            if (CAS(&(right_item->next), /*C3*/ &right_item_next,
					(item *) get_marked_reference(right_item_next)))
				break;

    } while (true); /*B4*/

    *found = true;

    if (!CAS(&(left_item->next), &right_item, right_item_next)) {/*C4*/
        right_item = (item*) get_unmarked_reference(right_item);
#ifdef MARK_REPLACEMENT
        right_item = search(list, ITEM_key(right_item), right_item->nkey, &left_item, false);
#else
        right_item = search(list, ITEM_key(right_item), right_item->nkey, &left_item);
#endif
        return NULL;
    }

    /* add removed item to be reclaimed */
    if(reclaim)
        ebr_add_retired_item(right_item, CUSTOM_TYPE);

    return right_item;
}


#ifdef MARK_REPLACEMENT //MARK REPLACEMENT------------------------------------------
//Will return NULL if item was not found
//  If item was not found, it was not inserted either, so if
//  the objective is to insert it, insert should be called
item* replace(List* list, const char* search_key, const size_t nkey, item *new_it,
    bool reclaim, bool *inserted) {

    item *right_item, *right_item_next, *left_item = NULL;
    item *old_it; //Item to remove
    *inserted = false;

    /* Mark old item as replaced */
    //Search for item to be replaced
    right_item = search(list, search_key, nkey, &left_item, false);
    if ((right_item == NULL) || (right_item == list->tail) ||
        (KEY_cmp(ITEM_key(right_item), search_key, right_item->nkey, nkey) != 0)) {
        //Item not found, try to do normal insert
		//	TODO: try insert from the items we already found
		//		  although this should be rare and have little impact
        *inserted = insert(list, new_it);
        return NULL;
    }

    do { /* Logically mark old item as "in replacement" */

    	//Mark as being replaced
    	right_item_next = right_item->next;

    	if (is_marked_reference(right_item_next) || is_marked_replacement_reference(right_item_next) ||
    	    CAS(&(right_item->next), &right_item_next,
    	        (item *) get_marked_replacement_reference(right_item_next)))
				break; //If item has already been logically deleted;
					   //	marked as "in replacement";
					   //	or our marking succeeded, continue.
						 
    } while (true);


    old_it = right_item;

	goto skip_search_1;

    do { /* Insertion of new item portion */

        //Search for old item, ignoring replacement
        right_item = search_by_ref(list, old_it, &left_item, true);
        assert(right_item == (item*) get_unmarked_reference(old_it) || right_item == list->tail);

        if ((right_item == list->tail) ||
        	(right_item != (item*) get_unmarked_reference(old_it)))
            return NULL; //Item concurrently removed, nothing else to do

		if((ITEM_cmp(left_item, new_it) == 0))
			return NULL; //Someone else inserted the item before us
						 //	it is up to the caller to choose if
						 //	the operation should be repeated

skip_search_1:

        //Insert new item
        new_it->next = right_item;
        if (CAS(&(left_item->next), &old_it, new_it))
            break;

    } while (true);

    *inserted = true;

    //Threads starting to traverse the list
    //  after this point will see the new item

    do { /* Logically delete old item */

		//right_item (same as old_it) won't change here,
		//	so no need for re-traversal

        //refresh right_item_next as it has been marked
        right_item_next = right_item->next;

        if (is_marked_reference(right_item_next) ||
            CAS(&(right_item->next), /*C3*/ &right_item_next,
			    (item *) get_marked_reference(right_item_next)))
            break; //If is already marked or marking successful, continue to next stage

    } while (true);

    /* Physically remove old item */
    if (!CAS(&(new_it->next), &right_item, get_unmarked_reference(right_item_next))) {/*C4*/
        cleanup(list);
        return NULL;
    }

    /* add removed item to be reclaimed */
    if(reclaim)
        ebr_add_retired_item(right_item, CUSTOM_TYPE);

    return right_item;
}


//Search by reference MUST delete logically removed items or it
//	allows not (logically) deleted items to be inserted next to
//	logically deleted items and causes wrongfull deletion
//
//At the momment ignore_replacement should always be true
//	as there are no scenarios where it shoulnd't be ignored
//Keeping it as an argument for possible future scenarios
item* search_by_ref(List* list, item* search_item, item **left_item,
    bool ignore_replacement) {

	//NULL because of warnings
	item *left_item_next = NULL, *right_item = NULL;

    int replace_retries = 0; //Number of times retried because of replace marked items

search_again:

    if(!ignore_replacement && right_item != NULL &&
        is_marked_replacement_reference(right_item->next)) {

        //We retried because of replace marking
        replace_retries++;

        if(replace_retries >= MAX_REPLACE_RETRIES) {
            //Thread replacing has likely crashed
            //  try and finish part of the job, i.e., delete old item
            del_by_ref(list, right_item, true);
            return NULL; //Did not find item, abort
        }
    }

	do {
        item *t = list->head;
        item *t_next = list->head->next; 
        int marked_counter = 0;

		/* 1: Find left_item and right_item */
        do {
            if (!is_marked_reference(t_next)) {
                (* left_item) = t;
                left_item_next = t_next;
                marked_counter = 0;
            } else {
                marked_counter++;
            }

            t = (item *) get_unmarked_reference(t_next);
            if (t == list->tail)
				break;
            t_next = t->next;
        } while (is_marked_reference(t_next) ||
            //Virtually the only difference between
			//	normal search and search_by_ref
            t != search_item); /*B1*/

        right_item = t; 
		/* 2: Check items are adjacent */
        if (left_item_next == right_item) {

            if ((right_item != list->tail) &&
                (is_marked_reference(right_item->next) ||
                (!ignore_replacement && is_marked_replacement_reference(right_item->next))))
                goto search_again; /*G1*/
			else
				return right_item; /*R1*/
		}

 		/* 3: Remove one or more marked items */
        if (CAS(&((*left_item)->next), &left_item_next, right_item)) { /*C1*/
            //Add one or more marked items to be reclaimed
            item *e = (item*) get_unmarked_reference(left_item_next);
            while(e != NULL && marked_counter > 0) {
                ebr_add_retired_item(e, CUSTOM_TYPE);
                assert(is_marked_reference(e->next));
                e = (item*) get_unmarked_reference(e->next);
                marked_counter--;
            }

            if ((right_item != list->tail) &&
                (is_marked_reference(right_item->next) ||
                (!ignore_replacement && is_marked_replacement_reference(right_item->next))))
				goto search_again; /*G2*/
            else
		      	return right_item; /*R2*/
		}

    } while (true); /*B2*/
}



//Same as del but search by ref
item* del_by_ref(List *list, item *to_del, bool reclaim) {
    item *right_item, *right_item_next, *left_item = NULL;

    do {
        right_item = search_by_ref(list, to_del, &left_item, true);

        if (right_item == list->tail)
            return NULL;

        right_item_next = right_item->next;

        if (is_marked_reference(right_item_next) ||
            CAS(&(right_item->next), &right_item_next,
                (item *) get_marked_reference(right_item_next)))
				break;

    } while (true); /*B4*/

    if (!CAS(&(left_item->next), &right_item, right_item_next)) {/*C4*/
        //Possibly add item removal to search_by_ref, but it should
        //  be ok to let normal search remove logically deleted items
        return NULL;
    }

    /* add removed item to be reclaimed */
    if(reclaim)
        ebr_add_retired_item(right_item, CUSTOM_TYPE);

    return right_item;
}
//------------------------------------------MARK REPLACEMENT

#else


//POSTERIOR INSERTION REPLACEMENT---------------------------
item* replace(List* list, const char* search_key, const size_t nkey, item *new_it,
    bool reclaim, bool *inserted) {

    item *right_item, *left_item = NULL;
	*inserted = false;

    do {
        right_item = search_last(list, search_key, nkey, &left_item);

        new_it->next = right_item;

		if(left_item == NULL || left_item->next == NULL)
			return NULL;

        if (CAS(&(left_item->next), &right_item, new_it)) {
			*inserted = true;
			break;
		}

    } while (true); /*B3*/

	bool found;
	return del(list, search_key, nkey, reclaim, &found);
}

//Simple search, with slight difference of searching
//	until current key is not greater than searched key
//
//	result: left and right item find last occurrence of a given key in a list
item* search_last(List* list, const char* search_key, const size_t nkey, item **left_item) {
	//NULL because of warnings
	item *left_item_next = NULL, *right_item;

search_again:
	do {
        item *t = list->head;
        item *t_next = list->head->next; 
        int marked_counter = 0;

		//Wether the last item traversal had the same that we are looking for
		bool last_item_equal = true;

		/* 1: Find left_item and right_item */
        do {
            if (!is_marked_reference(t_next)) {
                (* left_item) = t;
                left_item_next = t_next;
                marked_counter = 0;
            } else {
                marked_counter++;
			}

            t = (item *) get_unmarked_reference(t_next);
            if (t == list->tail)
				break;

            t_next = t->next;

        } while (is_marked_reference(t_next) ||
            //Compare keys
            ((last_item_equal = (KEY_cmp(ITEM_key(t), search_key, t->nkey, nkey) <= 0)))); /*B1*/


        right_item = t; 

		/* 2: Check items are adjacent */
        if (left_item_next == right_item) {

            if ((right_item != list->tail) && is_marked_reference(right_item->next)) {
                goto search_again; /*G1*/
			} else
				return right_item; /*R1*/
		}

 		/* 3: Remove one or more marked items */
        if (CAS(&((*left_item)->next), &left_item_next, right_item)) { /*C1*/
            //Add one or more marked items to be reclaimed
            item *e = (item*) get_unmarked_reference(left_item_next);
            while(e != NULL && marked_counter > 0) {
                ebr_add_retired_item(e, CUSTOM_TYPE);
                assert(is_marked_reference(e->next));
                e = (item*) get_unmarked_reference(e->next);
                marked_counter--;
            }

            if ((right_item != list->tail) && is_marked_reference(right_item->next))
				goto search_again; /*G2*/
            else
		      	return right_item; /*R2*/
		}

    } while (true); /*B2*/
}
//---------------------------POSTERIOR INSERTION REPLACEMENT
#endif





bool find(List *list, const char* search_key, const size_t nkey) {
    item *right_item, *left_item = NULL;

#ifdef MARK_REPLACEMENT
    right_item = search(list, search_key, nkey, &left_item, false);
#else
    right_item = search(list, search_key, nkey, &left_item);
#endif

    if ((right_item == NULL) || (right_item == list->tail) ||
        (KEY_cmp(ITEM_key(right_item), search_key, right_item->nkey, nkey) != 0)) {
		return false;
    } else {
		return true;
    }
}

item* get(List *list, const char* search_key, const size_t nkey) {
    item *right_item, *left_item = NULL;
#ifdef MARK_REPLACEMENT
    right_item = search(list, search_key, nkey, &left_item, false);
#else
    right_item = search(list, search_key, nkey, &left_item);
#endif

    if ((right_item == NULL) || (right_item == list->tail) ||
        (KEY_cmp(ITEM_key(right_item), search_key, right_item->nkey, nkey) != 0)) {
        return NULL;
    } else {
		return right_item;
    }
}

item* search_index(List* list, const int index, item **left_item, bool is_delete) {
	//NULL because of warnings
	item *left_item_next = NULL, *right_item;

search_again:
	do {
        item *t = list->head;
        item *t_next = list->head->next; 
        int marked_counter = 0;
        int i = -1; //-1 to account for head

		/* 1: Find left_item and right_item */
        do {
            if (!is_marked_reference(t_next)) {
                (* left_item) = t;
                left_item_next = t_next;
                marked_counter = 0;
            } else {
                marked_counter++;
            }

            t = (item *) get_unmarked_reference(t_next);
            if ((t == list->tail) || 
                (is_delete && t->next == list->tail))
				break;
            t_next = t->next;

        } while (++i < index || is_marked_reference(t_next));

        right_item = t; 
		/* 2: Check items are adjacent */
        if (left_item_next == right_item) {
            if ((right_item != list->tail) && is_marked_reference(right_item->next))
                goto search_again; /*G1*/
			else
				return right_item; /*R1*/
		}

 		/* 3: Remove one or more marked items */
        if (CAS(&((*left_item)->next), &left_item_next, right_item)) { /*C1*/
            //Add one or more marked items to be reclaimed
            item *e = (item*) get_unmarked_reference(left_item_next);
            while(e != NULL && marked_counter > 0) {
                ebr_add_retired_item(e, CUSTOM_TYPE);
                assert(is_marked_reference(e->next));
                e = (item*) get_unmarked_reference(e->next);
                marked_counter--;
            }

            if ((right_item != list->tail) && is_marked_reference(right_item->next))
				goto search_again; /*G2*/
            else
		      	return right_item; /*R2*/
		}

    } while (true); /*B2*/
}

item* get_index(List *list, const int index) {
    item *right_item, *left_item = NULL;
    right_item = search_index(list, index, &left_item, false);
    if ((right_item == list->tail))
		return NULL;
    else
		return right_item;
}

//Inserts item into index position of list
//If index is greater than list size, inserts into last position
bool insert_index(List *list, item* it, int index) {
    item *right_item, *left_item = NULL;
    do {
        right_item = search_index(list, index, &left_item, false);
        it->next = right_item;
        if (CAS(&(left_item->next), &right_item, it)) /*C2*/
            return true;
    } while (true); /*B3*/
}

item* del_index(List* list, const int index) {
    item *right_item, *right_item_next, *left_item = NULL;

    do {
        right_item = search_index(list, index, &left_item, true);
        if (right_item == list->tail)
            return NULL;

        right_item_next = right_item->next;

        if (!is_marked_reference(right_item_next))
            if (CAS(&(right_item->next), /*C3*/ &right_item_next,
					(item *) get_marked_reference(right_item_next)))
				break;

    } while (true); /*B4*/

    if (!CAS(&(left_item->next), &right_item, right_item_next)) {/*C4*/
        right_item = (item*) get_unmarked_reference(right_item);
        right_item = search_index(list, index, &left_item, true);
        return NULL;
    }

    /* add removed item to be reclaimed */
    ebr_add_retired_item(right_item, CUSTOM_TYPE);
    return right_item;
}


//Wrapper for insert_index to insert into list head
inline bool insert_head(List *list, item* it) {return insert_index(list, it, 0);}
//Wrapper for insert_index to insert into list tail
inline bool insert_tail(List *list, item* it) {return insert_index(list, it, INT_MAX);}

//Wrapper for del_index to delete from list head
inline item* del_head(List* list) {return del_index(list, 0);}
//Wrapper for del_index to delete from list tail
inline item* del_tail(List* list) {return del_index(list, INT_MAX);}

