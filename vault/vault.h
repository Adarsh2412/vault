#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h>
#include <pthread.h>
#include <sys/stat.h>
#include "blake3.h"

#define NONCE_SIZE 6               // Size of the nonce in bytes
#define RECORD_SIZE 16 
#define HASH_SIZE (RECORD_SIZE - NONCE_SIZE)
#define HASHGEN_THREADS_BUFFER (256 * 1024)

bool DEBUG = false;

size_t bucketSizeInBytes;
struct writeObject *buckets;

const int SEARCH_SIZE = 256;
const int NUM_BUCKETS = 256 * 256;      // Number of buckets
int BUCKET_SIZE = 1;            // Number of random records per bucket
unsigned long long FLUSH_SIZE = 1;
size_t BATCH_SIZE = 1;
int NUM_THREADS = 2;

unsigned long long NUM_ENTRIES = 64; // Number of random records to generate