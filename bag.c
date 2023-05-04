#include "bag.h"

bag* create_bag(int bag_size) {
    bag* b = malloc(sizeof(bag));
    b->elems = (NODE_TYPE**) malloc(sizeof(NODE_TYPE*) * bag_size);
    if(b->elems == NULL) {
        fprintf(stderr, "Could not allocate bag of size %d\n", bag_size);
        exit(EXIT_FAILURE);
    } 

    b->bag_size = bag_size;
    b->curr_in_bag = 0;
    return b;
}

void free_bag(bag* b) {
    free(b->elems);
    free(b);
}

//Insert new elem e in end of bag, grow if needed
int put(bag *b, NODE_TYPE* e) {
    int ret = 0;
    if(b->curr_in_bag == b->bag_size) {
        ret = 1;
        grow(b);
    }

    b->elems[b->curr_in_bag++] = e;
    return ret;
}

//Removes one element from bag
NODE_TYPE* take(bag *b) {
    NODE_TYPE* ret = NULL;
    if(b->curr_in_bag > 0)
        ret = b->elems[--b->curr_in_bag];
    return ret;
}

/* Transfers elements from src to dest
If dest's bag does not have enough space to hold src, dest grows
Removes src bag's items  */
int transfer(bag *dest, bag *src) {
    int ret = 0;

    while(dest->bag_size - dest->curr_in_bag < src->curr_in_bag) {
        ret = 1;
        grow(dest);
    }

    //Transfer elems
    memcpy(dest->elems + dest->curr_in_bag,
        src->elems, src->curr_in_bag * sizeof(NODE_TYPE*));

    dest->curr_in_bag += src->curr_in_bag; //Grow dest size
    src->curr_in_bag = 0; //*Empty* src

    return ret;
}

//Grows bag to GROWTH_FACTOR times its current size
void grow(bag *b) {
    b->bag_size = b->bag_size * GROWTH_FACTOR;
    b->elems = realloc(b->elems, b->bag_size * sizeof(NODE_TYPE*));
    if(b->elems == NULL) {
        fprintf(stderr, "Could not grow bag to size %d\n", b->bag_size);
        exit(EXIT_FAILURE);
    } 
}
