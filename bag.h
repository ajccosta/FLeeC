#ifndef BAG_H
#define BAG_H

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

//For ease of use (less casts)
#ifndef NODE_TYPE
    #define NODE_TYPE uintptr_t
#endif

#define GROWTH_FACTOR 2

typedef struct bag bag;
struct bag {
    int curr_in_bag; //number of elements currently in the bag
    int bag_size; //maximum number of elements in the bag
    NODE_TYPE** elems;
};

bag* create_bag(int bag_size);
void free_bag(bag* b);
int put(bag *b, NODE_TYPE* e);
NODE_TYPE* take(bag *b);
int transfer(bag *dest, bag *src);
void grow(bag *b);

#endif
