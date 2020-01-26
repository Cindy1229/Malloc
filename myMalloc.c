#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "myMalloc.h"
#include "printing.h"

/* Due to the way assert() prints error messges we use out own assert function
 * for deteminism when testing assertions
 */
#ifdef TEST_ASSERT
  inline static void assert(int e) {
    if (!e) {
      const char * msg = "Assertion Failed!\n";
      write(2, msg, strlen(msg));
      exit(1);
    }
  }
#else
  #include <assert.h>
#endif

/*
 * Mutex to ensure thread safety for the freelist
 */
static pthread_mutex_t mutex;

/*
 * Array of sentinel nodes for the freelists
 */
header freelistSentinels[N_LISTS];

/*
 * Pointer to the second fencepost in the most recently allocated chunk from
 * the OS. Used for coalescing chunks
 */
header * lastFencePost;

/*
 * Pointer to maintian the base of the heap to allow printing based on the
 * distance from the base of the heap
 */ 
void * base;

/*
 * List of chunks allocated by  the OS for printing boundary tags
 */
header * osChunkList [MAX_OS_CHUNKS];
size_t numOsChunks = 0;

/*
 * direct the compiler to run the init function before running main
 * this allows initialization of required globals
 */
static void init (void) __attribute__ ((constructor));

// Helper functions for manipulating pointers to headers
static inline header * get_header_from_offset(void * ptr, ptrdiff_t off);
static inline header * get_left_header(header * h);
static inline header * ptr_to_header(void * p);

// Helper functions for allocating more memory from the OS
static inline void initialize_fencepost(header * fp, size_t left_size);
static inline void insert_os_chunk(header * hdr);
static inline void insert_fenceposts(void * raw_mem, size_t size);
static header * allocate_chunk(size_t size);

// Helper functions for freeing a block
static inline void deallocate_object(void * p);

// Helper functions for allocating a block
static inline header * allocate_object(size_t raw_size);

// Helper functions for verifying that the data structures are structurally 
// valid
static inline header * detect_cycles();
static inline header * verify_pointers();
static inline bool verify_freelist();
static inline header * verify_chunk(header * chunk);
static inline bool verify_tags();

static void init();

static bool isMallocInitialized;

/**
 * @brief Helper function to retrieve a header pointer from a pointer and an 
 *        offset
 *
 * @param ptr base pointer
 * @param off number of bytes from base pointer where header is located
 *
 * @return a pointer to a header offset bytes from pointer
 */
static inline header * get_header_from_offset(void * ptr, ptrdiff_t off) {
	return (header *)((char *) ptr + off);
}

/**
 * @brief Helper function to get the header to the right of a given header
 *
 * @param h original header
 *
 * @return header to the right of h
 */
header * get_right_header(header * h) {
	return get_header_from_offset(h, get_block_size(h));
}

/**
 * @brief Helper function to get the header to the left of a given header
 *
 * @param h original header
 *
 * @return header to the right of h
 */
inline static header * get_left_header(header * h) {
  return get_header_from_offset(h, -h->left_size);
}

/**
 * @brief Fenceposts are marked as always allocated and may need to have
 * a left object size to ensure coalescing happens properly
 *
 * @param fp a pointer to the header being used as a fencepost
 * @param left_size the size of the object to the left of the fencepost
 */
inline static void initialize_fencepost(header * fp, size_t left_size) {
	set_block_state(fp,FENCEPOST);
	set_block_size(fp, ALLOC_HEADER_SIZE);
	fp->left_size = left_size;
}

/**
 * @brief Helper function to maintain list of chunks from the OS for debugging
 *
 * @param hdr the first fencepost in the chunk allocated by the OS
 */
inline static void insert_os_chunk(header * hdr) {
  if (numOsChunks < MAX_OS_CHUNKS) {
    osChunkList[numOsChunks++] = hdr;
  }
}

/**
 * @brief given a chunk of memory insert fenceposts at the left and 
 * right boundaries of the block to prevent coalescing outside of the
 * block
 *
 * @param raw_mem a void pointer to the memory chunk to initialize
 * @param size the size of the allocated chunk
 */
inline static void insert_fenceposts(void * raw_mem, size_t size) {
  // Convert to char * before performing operations
  char * mem = (char *) raw_mem;

  // Insert a fencepost at the left edge of the block
  header * leftFencePost = (header *) mem;
  initialize_fencepost(leftFencePost, ALLOC_HEADER_SIZE);

  // Insert a fencepost at the right edge of the block
  header * rightFencePost = get_header_from_offset(mem, size - ALLOC_HEADER_SIZE);
  initialize_fencepost(rightFencePost, size - 2 * ALLOC_HEADER_SIZE);
}

/**
 * @brief Allocate another chunk from the OS and prepare to insert it
 * into the free list
 *
 * @param size The size to allocate from the OS
 *
 * @return A pointer to the allocable block in the chunk (just after the 
 * first fencpost)
 */
static header * allocate_chunk(size_t size) {
  void * mem = sbrk(size);
  
  insert_fenceposts(mem, size);
  header * hdr = (header *) ((char *)mem + ALLOC_HEADER_SIZE);
  set_block_state(hdr, UNALLOCATED);
  set_block_size(hdr, size - 2 * ALLOC_HEADER_SIZE);
  hdr->left_size = ALLOC_HEADER_SIZE;
  return hdr;
}

/**
 * Helper to get the freelist index of a header
 */
static inline size_t get_index(header* head) {
  size_t index = 0;
  size_t raw_size = get_block_size(head) - ALLOC_HEADER_SIZE;

  index = raw_size/8 - 1;
  if (index > N_LISTS - 1) {
    index = N_LISTS - 1;
  }
  return index;
}

/**
 * Helper to insert block to given freelist
 */
static inline void insert_list(size_t index, header* ptr) {
  header * head = &freelistSentinels[index];
  ptr->next = head->next;
  ptr->prev = head;
  head->next->prev = ptr;
  head->next = ptr;
}



/**
 * @brief Helper allocate an object given a raw request size from the user
 *
 * @param raw_size number of bytes the user needs
 *
 * @return A block satisfying the user's request
 */
static inline header * allocate_object(size_t raw_size) {
  // TODO implement allocation


  // Return ptr to data field of the header
  header * ret = NULL;


  // Check if raw_size is 0
  if (raw_size <= 0) {
    return NULL;
  }

  // Round to nearest 8 and compute actual size
  size_t round = (raw_size + 7) & ~(7);
  size_t actual_size = round + ALLOC_HEADER_SIZE;

  if (actual_size < sizeof(header)) {
    actual_size = sizeof(header);
  }

  // A flag to inidicate if there is enough memory
  size_t flag = 0;


  // Calculate which freelist to allocate the object
  size_t index = 0;

  if (round >= (N_LISTS - 1) * MIN_ALLOCATION +1) {
    index = N_LISTS - 1;
  }
  else {
    index = round / 8 - 1;
  }

  // Find the first freelist that is not empty
  header * list = &freelistSentinels[index];
  while (index < N_LISTS - 1) {
    if (list->next != list) {
      break;
    }
    index++;
    list = &freelistSentinels[index];
  }

  // When freelist index is not N_LISTS - 1
  if (index != N_LISTS - 1) {
    header* head = &freelistSentinels[index];

    header* node = head->next;


    size_t b_size = get_block_size(node);


    // CASE1 Block not enough to split, just remove the block
    if ((b_size >= actual_size) && (b_size - actual_size) <  sizeof(header)) {
      //Set the block as allocated and remove the block from freelist
      set_block_state(node, ALLOCATED);

      node->next->prev = node->prev;
      node->prev->next = node->next;

      // Set ret to data ptr
      ret =(header*) ((char*) node + ALLOC_HEADER_SIZE);
      return ret;
    }

    // CASE2 split into 2
    else if ((b_size - actual_size) >= sizeof(header)) {
      //Debug print
      //printf("split into 2 from index %ld\n", index);

      //Set pointer to where to split
      char *temp = (char*) node + b_size - actual_size;

      //Set the remainder block
      set_block_size(node, b_size - actual_size);
      set_block_state(node, UNALLOCATED);

      //Set the allocated block
      header * split = (header*) temp;
      set_block_size(split, actual_size);
      set_block_state(split, ALLOCATED);

      //Update split block's next bloack's left_size
      temp = temp + actual_size;
      header * right = (header*) temp;
      right->left_size = actual_size;

      // Update split left_size
      split->left_size = get_block_size(node);

      // If remainder block size still fits in the current list
      // allocated block left_size should be remainder block size
      size_t r_size = b_size - actual_size - ALLOC_HEADER_SIZE;

      // Remove the remainder from the list
      node->next->prev = node->prev;
      node->prev->next = node->next;

      // Insert the remainder to another list
      size_t n_index = r_size / 8 - 1;
      insert_list(n_index, node);
      //Debug print
      //printf("inserting remainder to list %ld\n", n_index);


      // Set ret ptr to split
      ret = (header*) ((char*)split + ALLOC_HEADER_SIZE);
      return ret;
    }
  }

  // If index is N_LIST -1
  else {

    while (1) {
      header * head = &freelistSentinels[N_LISTS - 1];
      header * node = head->next;
      // Find the fisrt fit block
      while (node != head) {
        // Set flag to 0, means that there's enough memory
        flag = 0;

        size_t b_size = get_block_size(node);
        if (b_size >= actual_size) {
          break;
        }
        node = node ->next;
      }

      //If no enough memory set flag to 1
      if (node == head) {
        flag = 1;
      }

      if (flag == 1) {
        //printf("need more os chunk call the function\n");
        node = allocate_chunk(ARENA_SIZE);
        header* fence1 = get_header_from_offset(node, -ALLOC_HEADER_SIZE);
        //insert_os_chunk(fence1);
        header* fence2 = get_header_from_offset(node, get_block_size(node));

        // Get the addr of left of  the first fencepost
        header* left_fence1 = get_left_header(fence1);

        // If the new chunk is contiguous, merge left fencepost
        if ((char*) lastFencePost == (char*) left_fence1) {
          //Debug printf
          //printf("==================================Contiguous block============================\n");

          // Update last fence post
          lastFencePost = fence2;

          // Merge the left_fence1 with node and set head to left_fence1
          size_t new_size = get_block_size(node) + 2 * ALLOC_HEADER_SIZE;
          set_block_size(left_fence1, new_size);
          set_block_state(left_fence1, UNALLOCATED);

          // If the left of left_fence1 is free, merge that with the new chunk
          header* l_left = get_left_header(left_fence1);
          // Get the freelist index of l_left
          size_t o_index = get_index(l_left);

          if (get_block_state(l_left) == UNALLOCATED) {
            // Merge with left
            new_size = get_block_size(left_fence1) + get_block_size(l_left);
            set_block_size(l_left, new_size);

            // Update fence2 left_size
            fence2->left_size = new_size;

            // If l_list is not originally in N_LISTS - 1, remove it
            if (o_index != N_LISTS - 1) {
              // Remove l_left from original freelist and insert to N_LISTS-1
              l_left->next->prev = l_left->prev;
              l_left->prev->next = l_left->next;
              insert_list(N_LISTS - 1, l_left);
            }

            // Update node
            node = l_left;
          }

          else {
          // Update fence2 left size
          fence2->left_size = new_size;

          // Insert the new chunk to N_LISTS-1
          insert_list(N_LISTS - 1, left_fence1);

          // Update Node
          node = left_fence1;

          }
        }

        // If not contiguous, just add the new chunk to N_LISTS - 1
        else {
          // Update the last fencepost
          //printf("=======================NOT CONTIGUOUS======================\n");
          //printf("lastfence: %p, fence1: %p\n", (char*) lastFencePost, (char*) left_fence1);
          lastFencePost = fence2;

          insert_os_chunk(fence1);

          insert_list(N_LISTS - 1, node);
        }
        flag = 0;
      }

      if (!flag) {
        // CASE1 Block not enough to split, just remove the block
        size_t b_size = get_block_size(node);
        if ((b_size >= actual_size) && (b_size - actual_size) <  sizeof(header)) {
          //Set the block as allocated and remove the block from freelist
          set_block_state(node, ALLOCATED);

          node->next->prev = node->prev;
          node->prev->next = node->next;

          // Set ret to data ptr
          ret =(header*) ((char*) node + ALLOC_HEADER_SIZE);
          return ret;
        }

        // CASE2 split into 2
        else if ((b_size - actual_size) >= sizeof(header)) {
          //printf("here\n");
          //Set pointer to where to split
          char *temp = (char*) node + b_size - actual_size;

          //Set the remainder block
          set_block_size(node, b_size - actual_size);
          set_block_state(node, UNALLOCATED);

          //Set the allocated block
          header * split = (header*) temp;
          set_block_size(split, actual_size);
          set_block_state(split, ALLOCATED);

          //Update split block's next bloack's left_size
          temp = temp + actual_size;
          header * right = (header*) temp;
          right->left_size = actual_size;

          // Update split block's left_size
          split->left_size = get_block_size(node);

          // If remainder block size still fits in the current list
          // allocated block left_size should be remainder block size
          size_t r_size = b_size - actual_size - ALLOC_HEADER_SIZE;
          if (r_size >= MIN_ALLOCATION * (N_LISTS - 1) + 1) {
            split->left_size = get_block_size(node);
          }
          else {
            //Debug print
            //printf("Need to reinsert the remainder from N_LISTS-1\n");

            // Set split block left_size and relink remainder ptr 
            split->left_size = get_block_size(node);

            //Remove the remainder form current list
            node->next->prev = node->prev;
            node->prev->next = node->next;

            // Insert the remainder to another list
            size_t n_index = r_size / 8 - 1;
            insert_list(n_index, node);
            // Debug print
            //printf("new list to insert is %ld\n", n_index);
          }

          // Set ret ptr to split
          ret = (header*) ((char*)split + ALLOC_HEADER_SIZE);
          return ret;

        }

      }// END SPLIT

      //printf("More memory needed\n");
      break;
    }// END WHILE
  }

/*(void) raw_size;
  assert(false);
  exit(1);*/

  // Debug print
  //printf("need more memory, flag is %ld\n", flag);

}

/**
 * @brief Helper to get the header from a pointer allocated with malloc
 *
 * @param p pointer to the data region of the block
 *
 * @return A pointer to the header of the block
 */
static inline header * ptr_to_header(void * p) {
  return (header *)((char *) p - ALLOC_HEADER_SIZE); //sizeof(header));
}

/**
 * @brief Helper to manage deallocation of a pointer returned by the user
 *
 * @param p The pointer returned to the user by a call to malloc
 */
static inline void deallocate_object(void * p) {
  // TODO implement deallocation

  // Check if p is NULL
  if (p == NULL) {
    return;
  }
  //printf("free is called\n");

  // Calculate the header for p, and left and right header
  header* ptr = ptr_to_header(p);
  // If p is already freed, just return
  if (get_block_state(ptr) == UNALLOCATED) {
    //Debug print
    printf("Double Free Detected\n");
    assert(false);
    return;
  }

  header* left = get_left_header(ptr);
  header* right = get_right_header(ptr);

  // Check the state of left and right
  size_t left_alloc = 0;
  size_t right_alloc = 0;

  if (get_block_state(left) != UNALLOCATED) {
    left_alloc = 1;
  }
  if (get_block_state(right) != UNALLOCATED) {
    right_alloc = 1;
  }

  //Debug printf
  //printf("here\n");

  //Debug print
  //printf("right is %ld, left is %ld\n", right_alloc, left_alloc);

  // Get the freelist index of left and right
  size_t l_index = get_index(left);
  size_t r_index = get_index(right);

  // CASE1 merge neither
  if (left_alloc && right_alloc) {
    //Debug print
    //printf("merge neither\n");

    // Insert the block to its appropiate list
    size_t index = get_index(ptr);
    insert_list(index, ptr);
    set_block_state(ptr, UNALLOCATED);

  }
  // CASE2 merge right
  else if (left_alloc && !right_alloc) {
    //Debug print
    //printf("merge right\n");

    //Merge with right and update the size
    size_t new_size = get_block_size(right) + get_block_size(ptr);
    set_block_size(ptr, new_size);

    // Relink the freelist
    ptr->next = right->next;
    ptr->prev = right->prev;
    right->prev->next = ptr;
    right->next->prev = ptr;

    // Update state
    set_block_state(ptr, UNALLOCATED);

    // Update left_size of right's right
    header * r_right  = (header*) ((char*) right + get_block_size(right));
    r_right->left_size = new_size;

    // Insert the block to appropiate freelist
    size_t index = get_index(ptr);
    if (r_index != N_LISTS - 1 && index != r_index) {
      //Remove the block from original list
      ptr->next->prev = ptr->prev;
      ptr->prev->next = ptr->next;
      // Insert to the new freelist
      insert_list(index, ptr);
    }

  }
  // CASE3 merge left
  else if (!left_alloc && right_alloc) {
    // DEbug print
    //printf("merge left\n");

    // Merge with left and update the size
    size_t new_size = get_block_size(left) + get_block_size(ptr);
    //Try
    set_block_state(ptr, UNALLOCATED);

    set_block_size(left, new_size);
    set_block_state(left, UNALLOCATED);

    // Update right block left_size
    right->left_size = new_size;

    // Insert block to appropiate freelist
    size_t index = get_index(left);

    //Debug printf
    //printf("index to insert is %ld, l_index is %ld\n", index, l_index);
    if (l_index != N_LISTS - 1 && index != l_index) {
      // Remove the block from original freelist
      left->next->prev = left->prev;
      left->prev->next = left->next;
      // Insert into the new list
      insert_list(index, left);
    }


  }
  // CASE4 merge both
  else {
    // Debug print
    //printf("merge both\n");

    // Merge both and update new_size
    size_t new_size = get_block_size(left) + get_block_size(right) + get_block_size(ptr);
    set_block_state(ptr, UNALLOCATED);

    set_block_size(left, new_size);
    set_block_state(left, UNALLOCATED);

    // Remove right from list
    right->prev->next = right->next;
    right->next->prev = right->prev;

    // Update right's right left_size
    header * r_right  = (header*) ((char*) right + get_block_size(right));
    r_right->left_size = new_size;

    // Insert the block to appropiate list
    size_t index = get_index(left);
    if (l_index != N_LISTS - 1 && index != l_index) {
      insert_list(index, left);
    }
  }


 /* (void) p;
  assert(false);
  exit(1);*/
}

/**
 * @brief Helper to detect cycles in the free list
 * https://en.wikipedia.org/wiki/Cycle_detection#Floyd's_Tortoise_and_Hare
 *
 * @return One of the nodes in the cycle or NULL if no cycle is present
 */
static inline header * detect_cycles() {
  for (int i = 0; i < N_LISTS; i++) {
    header * freelist = &freelistSentinels[i];
    for (header * slow = freelist->next, * fast = freelist->next->next; 
         fast != freelist; 
         slow = slow->next, fast = fast->next->next) {
      if (slow == fast) {
        return slow;
      }
    }
  }
  return NULL;
}

/**
 * @brief Helper to verify that there are no unlinked previous or next pointers
 *        in the free list
 *
 * @return A node whose previous and next pointers are incorrect or NULL if no
 *         such node exists
 */
static inline header * verify_pointers() {
  for (int i = 0; i < N_LISTS; i++) {
    header * freelist = &freelistSentinels[i];
    for (header * cur = freelist->next; cur != freelist; cur = cur->next) {
      if (cur->next->prev != cur || cur->prev->next != cur) {
        return cur;
      }
    }
  }
  return NULL;
}

/**
 * @brief Verify the structure of the free list is correct by checkin for 
 *        cycles and misdirected pointers
 *
 * @return true if the list is valid
 */
static inline bool verify_freelist() {
  header * cycle = detect_cycles();
  if (cycle != NULL) {
    fprintf(stderr, "Cycle Detected\n");
    print_sublist(print_object, cycle->next, cycle);
    return false;
  }

  header * invalid = verify_pointers();
  if (invalid != NULL) {
    fprintf(stderr, "Invalid pointers\n");
    print_object(invalid);
    return false;
  }

  return true;
}

/**
 * @brief Helper to verify that the sizes in a chunk from the OS are correct
 *        and that allocated node's canary values are correct
 *
 * @param chunk AREA_SIZE chunk allocated from the OS
 *
 * @return a pointer to an invalid header or NULL if all header's are valid
 */
static inline header * verify_chunk(header * chunk) {
	if (get_block_state(chunk) != FENCEPOST) {
		fprintf(stderr, "Invalid fencepost\n");
		print_object(chunk);
		return chunk;
	}
	
	for (; get_block_state(chunk) != FENCEPOST; chunk = get_right_header(chunk)) {
		if (get_block_size(chunk)  != get_right_header(chunk)->left_size) {
			fprintf(stderr, "Invalid sizes\n");
			print_object(chunk);
			return chunk;
		}
	}
	
	return NULL;
}

/**
 * @brief For each chunk allocated by the OS verify that the boundary tags
 *        are consistent
 *
 * @return true if the boundary tags are valid
 */
static inline bool verify_tags() {
  for (size_t i = 0; i < numOsChunks; i++) {
    header * invalid = verify_chunk(osChunkList[i]);
    if (invalid != NULL) {
      return invalid;
    }
  }

  return NULL;
}

/**
 * @brief Initialize mutex lock and prepare an initial chunk of memory for allocation
 */
static void init() {
  // Initialize mutex for thread safety
  pthread_mutex_init(&mutex, NULL);

#ifdef DEBUG
  // Manually set printf buffer so it won't call malloc when debugging the allocator
  setvbuf(stdout, NULL, _IONBF, 0);
#endif // DEBUG

  // Allocate the first chunk from the OS
  header * block = allocate_chunk(ARENA_SIZE);

  header * prevFencePost = get_header_from_offset(block, -ALLOC_HEADER_SIZE);
  insert_os_chunk(prevFencePost);

  lastFencePost = get_header_from_offset(block, get_block_size(block));

  // Set the base pointer to the beginning of the first fencepost in the first
  // chunk from the OS
  base = ((char *) block) - ALLOC_HEADER_SIZE; //sizeof(header);

  // Initialize freelist sentinels
  for (int i = 0; i < N_LISTS; i++) {
    header * freelist = &freelistSentinels[i];
    freelist->next = freelist;
    freelist->prev = freelist;
  }

  // Insert first chunk into the free list
  header * freelist = &freelistSentinels[N_LISTS - 1];
  freelist->next = block;
  freelist->prev = block;
  block->next = freelist;
  block->prev = freelist;
}

/* 
 * External interface
 */
void * my_malloc(size_t size) {
  pthread_mutex_lock(&mutex);
  header * hdr = allocate_object(size); 
  pthread_mutex_unlock(&mutex);
  return hdr;
}

void * my_calloc(size_t nmemb, size_t size) {
  return memset(my_malloc(size * nmemb), 0, size * nmemb);
}

void * my_realloc(void * ptr, size_t size) {
  void * mem = my_malloc(size);
  memcpy(mem, ptr, size);
  my_free(ptr);
  return mem; 
}

void my_free(void * p) {
  pthread_mutex_lock(&mutex);
  deallocate_object(p);
  pthread_mutex_unlock(&mutex);
}

bool verify() {
  return verify_freelist() && verify_tags();
}
