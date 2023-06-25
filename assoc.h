/* associative array */
void assoc_init(const int hashpower_init);

item *assoc_find(const char *key, const size_t nkey, const uint32_t hv);
int assoc_insert(item *item, const uint32_t hv);
int assoc_delete(const char *key, const size_t nkey, const uint32_t hv);

int assoc_replace(item *old_it, item *new_it, const uint32_t hv);
void assoc_bump(item *it, const uint32_t hv);
int try_evict(const int orig_id, const uint64_t total_bytes, const rel_time_t max_age);

uint64_t get_curr_items(void);

int start_assoc_maintenance_thread(ebr *r);
void assoc_check_expand(void);
void *assoc_maintenance_thread(void *arg);
void start_expansion(void);

extern __thread int tid;
extern volatile unsigned int hashpower;
