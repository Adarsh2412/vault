#include "vault.h"

// Structure to hold a 16-byte random record
typedef struct {
    uint8_t hash[HASH_SIZE]; // Actual random bytes
    uint8_t nonce[NONCE_SIZE];              // Nonce to store the seed (4-byte unsigned integer)
} MemoRecord;

// Structure to hold a bucket of random records
typedef struct {
    MemoRecord *records;
    size_t count; // Number of random records in the bucket
    size_t flush; // Number of flushes of bucket
} Bucket;

typedef struct {
    struct timeval start;
    struct timeval end;
} Timer;

void resetTimer(Timer *timer) {
    gettimeofday(&timer->start, NULL);
}

double getTimer(Timer *timer) {
    gettimeofday(&timer->end, NULL);
    return (timer->end.tv_sec - timer->start.tv_sec) + (timer->end.tv_usec - timer->start.tv_usec) / 1000000.0;
}

// Function to compare two random records for sorting
int compareMemoRecords(const void *a, const void *b) {
    const MemoRecord *ra = (const MemoRecord *)a;
    const MemoRecord *rb = (const MemoRecord *)b;
    return memcmp(ra->hash, rb->hash, sizeof(ra->hash));
}

struct CircularArray {
    //unsigned char array[HASHGEN_THREADS_BUFFER][HASH_SIZE];
    MemoRecord array[HASHGEN_THREADS_BUFFER];
    size_t head;
    size_t tail;
    int producerFinished;  // Flag to indicate when the producer is finished
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
};

void initCircularArray(struct CircularArray *circularArray) {
    circularArray->head = 0;
    circularArray->tail = 0;
    circularArray->producerFinished = 0;
    pthread_mutex_init(&(circularArray->mutex), NULL);
    pthread_cond_init(&(circularArray->not_empty), NULL);
    pthread_cond_init(&(circularArray->not_full), NULL);
}

void destroyCircularArray(struct CircularArray *circularArray) {
    pthread_mutex_destroy(&(circularArray->mutex));
    pthread_cond_destroy(&(circularArray->not_empty));
    pthread_cond_destroy(&(circularArray->not_full));
}

void insertBatch(struct CircularArray *circularArray, MemoRecord values[BATCH_SIZE]) {
	if (DEBUG)
	printf("insertBatch(): before mutex lock\n");
    pthread_mutex_lock(&(circularArray->mutex));

	if (DEBUG)
	printf("insertBatch(): Wait while the circular array is full and producer is not finished\n");
    // Wait while the circular array is full and producer is not finished
    while ((circularArray->head + BATCH_SIZE) % HASHGEN_THREADS_BUFFER == circularArray->tail && !circularArray->producerFinished) {
        pthread_cond_wait(&(circularArray->not_full), &(circularArray->mutex));
    }

	if (DEBUG)
	printf("insertBatch(): Insert values\n");
    // Insert values
    for (int i = 0; i < BATCH_SIZE; i++) {
        memcpy(&circularArray->array[circularArray->head], &values[i], sizeof(MemoRecord));
        circularArray->head = (circularArray->head + 1) % HASHGEN_THREADS_BUFFER;
    }

 	if (DEBUG)
 	printf("insertBatch(): Signal that the circular array is not empty\n");
   // Signal that the circular array is not empty
    pthread_cond_signal(&(circularArray->not_empty));

 	if (DEBUG)
 	printf("insertBatch(): mutex unlock\n");
    pthread_mutex_unlock(&(circularArray->mutex));
}

void removeBatch(struct CircularArray *circularArray, MemoRecord *result) {
    pthread_mutex_lock(&(circularArray->mutex));

    // Wait while the circular array is empty and producer is not finished
    while (circularArray->tail == circularArray->head && !circularArray->producerFinished) {
        pthread_cond_wait(&(circularArray->not_empty), &(circularArray->mutex));
    }

    // Remove values
    for (int i = 0; i < BATCH_SIZE; i++) {
        memcpy(&result[i], &circularArray->array[circularArray->tail], sizeof(MemoRecord));
        circularArray->tail = (circularArray->tail + 1) % HASHGEN_THREADS_BUFFER;
    }

    // Signal that the circular array is not full
    pthread_cond_signal(&(circularArray->not_full));

    pthread_mutex_unlock(&(circularArray->mutex));
}

// Thread data structure
struct ThreadData {
    struct CircularArray *circularArray;
    int threadID;
};

// Function to generate a pseudo-random record using BLAKE3 hash
void generateBlake3(MemoRecord *record, unsigned long long seed) {
    // Store seed into the nonce
    memcpy(record->nonce, &seed, sizeof(record->nonce));

    // Generate random bytes
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, &record->nonce, sizeof(record->nonce));
    blake3_hasher_finalize(&hasher, record->hash, RECORD_SIZE - NONCE_SIZE);
}


// Function to be executed by each thread for array generation
void *arrayGenerationThread(void *arg) {
    struct ThreadData *data = (struct ThreadData *)arg;
	if (DEBUG)
		printf("arrayGenerationThread %d\n",data->threadID);
	int hashObjectSize = sizeof(MemoRecord);
    //unsigned char batch[BATCH_SIZE][HASH_SIZE];
    MemoRecord batch[BATCH_SIZE];
    long long NUM_HASHES_PER_THREAD = (long long)(NUM_ENTRIES / NUM_THREADS);
	unsigned char hash[HASH_SIZE];
	unsigned long long hashIndex = 0;
	long long i = 0;
    while(data->circularArray->producerFinished == 0)
    {
    	if (DEBUG)
    	printf("arrayGenerationThread(), inside while loop %llu...\n",i);
    //for (unsigned int i = 0; i < NUM_HASHES_PER_THREAD; i += BATCH_SIZE) {
        for (long long j = 0; j < BATCH_SIZE; j++) {
        if (DEBUG)
        printf("arrayGenerationThread(), inside for loop %llu...\n",j);
            hashIndex = (long long)(NUM_HASHES_PER_THREAD * data->threadID + i + j);
            generateBlake3(&batch[j], hashIndex);
            //batch[j].prefix = byteArrayToInt(hash,0);
            //memcpy(batch[j].data.byteArray, hash+PREFIX_SIZE, HASH_SIZE-PREFIX_SIZE);
            //batch[j].data.NONCE = hashIndex;
        }
		//should add hashIndex as NONCE to hashObject
		if (DEBUG)
		printf("insertBatch()...\n");
        insertBatch(data->circularArray, batch);
        i += BATCH_SIZE;
    }

	if (DEBUG)	
		printf("finished generating hashes on thread id %d, thread exiting...\n",data->threadID);
    return NULL;
}

// Function to write a bucket of random records to disk
void writeBucketToDisk(const Bucket *bucket, int fd, off_t offset) {
    if (lseek(fd, offset * sizeof(MemoRecord) * BUCKET_SIZE * FLUSH_SIZE + sizeof(MemoRecord) * BUCKET_SIZE * bucket->flush, SEEK_SET) < 0) {
        printf("writeBucketToDisk(): Error seeking in file at offset %llu; more details: %llu %llu %lu %d %zu\n",offset * sizeof(MemoRecord) * BUCKET_SIZE * FLUSH_SIZE + sizeof(MemoRecord) * BUCKET_SIZE * bucket->flush,offset,FLUSH_SIZE,sizeof(MemoRecord),BUCKET_SIZE,bucket->flush);
        close(fd);
        exit(EXIT_FAILURE);
    }

    // Write the entire bucket to the file
    if (write(fd, bucket->records, sizeof(MemoRecord) * bucket->count) < 0) {
        perror("Error writing to file");
        close(fd);
        exit(EXIT_FAILURE);
    }
}


off_t getBucketIndex(const uint8_t *hash) {
    // Calculate the bucket index based on the first 2-byte prefix
    return (hash[0] << 8) | hash[1];
}

void printBytes(const uint8_t *bytes, size_t length)
{
    for (size_t i = 0; i < length; i++)
    {
        printf("%02x", bytes[i]);
    }
}

// Function to print the contents of the file
void printFile(const char *filename, int numRecords) {
    FILE *file = fopen(filename, "rb");
    if (file == NULL) {
        printf("Error opening file for reading!\n");
        return;
    }

    MemoRecord number;
    // uint8_t array[16]; 

    unsigned long long recordsPrinted = 0;
    
    while (recordsPrinted < numRecords && fread(&number, sizeof(MemoRecord), 1, file) == 1) 
    {
        // Interpret nonce as unsigned long long
        unsigned long long nonceValue = 0;
        for (int i = 0; i < sizeof(number.nonce); i++) {
            nonceValue |= (unsigned long long)number.nonce[i] << (i * 8);
        }

        // Print hash
        printf("[%llu] Hash: ",recordsPrinted*sizeof(MemoRecord));
        for (int i = 0; i < sizeof(number.hash); i++) {
            printf("%02x", number.hash[i]);
        }
        printf(" : ");

        // Print nonce
        for (int i = 0; i < sizeof(number.nonce); i++) {
            printf("%02x", number.nonce[i]);
        }

        // Print nonce as unsigned long long
        printf(" : %llu\n", nonceValue);

        recordsPrinted++;
        
    }
	//printf("all done!\n");
	
    fclose(file);
}

long long getFileSize(const char *filename)
{
    struct stat st;

    if (stat(filename, &st) == 0)
    {
        return st.st_size;
    }
    else
    {
        perror("Error getting file size");
        return -1; // Return -1 to indicate an error
    }
}

// Binary search function to search for a hash from disk
int binarySearch(const uint8_t *targetHash, size_t targetLength, int fileDescriptor, long long filesize, int *seekCount, bool bulk)
{
	if (DEBUG)
	{
	printf("binarySearch()=");
        		printBytes(targetHash, targetLength);    
        		printf("\n");
        		}
//should use filesize to determine left and right
  // Calculate the bucket index based on the first 2-byte prefix
        long long bucketIndex = (targetHash[0] << 8) | targetHash[1];
        if (DEBUG)
        printf("bucketIndex=%lld\n",bucketIndex);
        
        
        //filesize
	long long FILESIZE = filesize;
	if (DEBUG)
		printf("FILESIZE=%lld\n", FILESIZE);

	//int RECORD_SIZE = sizeof(MemoRecord);
	if (DEBUG)
		printf("RECORD_SIZE=%d\n", RECORD_SIZE);
	

	unsigned long long NUM_ENTRIES = FILESIZE/RECORD_SIZE;
    if (DEBUG)
    	printf("NUM_ENTRIES=%lld\n", NUM_ENTRIES);
	
	int BUCKET_SIZE = (FILESIZE)/(RECORD_SIZE*NUM_BUCKETS);
	//BUCKET_SIZE = 1024;
    if (DEBUG)
    	printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);
    
 //left and right are record numbers, not byte offsets
     off_t left = bucketIndex*BUCKET_SIZE;
    if (DEBUG)
    	printf("left=%lld\n",left);
    off_t right = (bucketIndex+1)*BUCKET_SIZE-1;
    if (DEBUG)
    	printf("right=%lld\n",right);     
     
     
    //off_t right = filesize / sizeof(MemoRecord);
    off_t middle;
    MemoRecord number;

	if (bulk == false)
    	*seekCount = 0; // Initialize seek count

    while (left <= right && right - left > SEARCH_SIZE)
    {
        middle = left + (right - left) / 2;
    	if (DEBUG)
    		printf("left=%lld middle=%lld right=%lld\n",left, middle, right);

        // Increment seek count
        (*seekCount)++;
    	//if (DEBUG)
    	//	printf("seekCount=%lld \n",seekCount);

		if (DEBUG)
			printf("lseek=%lld %lu\n",middle*sizeof(MemoRecord),sizeof(MemoRecord));
        // Seek to the middle position
        if (lseek(fileDescriptor, middle*sizeof(MemoRecord), SEEK_SET) < 0)
        {
            printf("binarySearch(): Error seeking in file at offset %llu\n",middle*sizeof(MemoRecord));
            exit(EXIT_FAILURE);
        }

        // Read the hash at the middle position
        if (read(fileDescriptor, &number, sizeof(MemoRecord)) < 0)
        {
            perror("Error reading from file");
            exit(EXIT_FAILURE);
        }

        // Compare the target hash with the hash read from file
        
        if (DEBUG)
        {
		printf("memcmp(targetHash)=");
        		printBytes(targetHash, targetLength);    
        		printf("\n");        
		printf("memcmp(number.hash)=");
        		printBytes(number.hash, targetLength);    
        		printf("\n");        
        		}
        int cmp = memcmp(targetHash, number.hash, targetLength);
        
        if (DEBUG)
        {
        printf("memcmp()=%d\n",cmp);
        printf("nonce=");
        // Print nonce
        for (int i = 0; i < sizeof(number.nonce); i++) {
            printf("%02x", number.nonce[i]);
        }
        printf("\n"); 
        }

        if (cmp == 0)
        {
            // Hash found
            return middle;
        }
        else if (cmp < 0)
        {
            // Search the left half
            right = middle - 1;
        }
        else
        {
            // Search the right half
            left = middle + 1;
        }
    }
    
// If the remaining data to search is less than 1024 bytes, perform a brute force search
    if (right - left <= SEARCH_SIZE)
    {
    // Increment seek count
        (*seekCount)++;
        // Seek to the left position
        if (lseek(fileDescriptor, left*sizeof(MemoRecord), SEEK_SET) < 0)
        {
            printf("binarySearch(2): Error seeking in file at offset %llu\n",left*sizeof(MemoRecord));
            exit(EXIT_FAILURE);
        }

        // Perform a brute force search in the remaining 1024 bytes
        while (left <= right)
        {
            // Read the hash at the current position
            if (read(fileDescriptor, &number, sizeof(MemoRecord)) < 0)
            {
                perror("Error reading from file");
                exit(EXIT_FAILURE);
            }

            // Compare the target hash with the hash read from file
            int cmp = memcmp(targetHash, number.hash, targetLength);
            if (cmp == 0)
            {
                // Hash found
                return left;
            }

            // Move to the next position
            left++;
        }
    }    

    // Hash not found
    return -1;
}

uint8_t *hexStringToByteArray(const char *hexString, uint8_t *byteArray, size_t byteArraySize)
{
    size_t hexLen = strlen(hexString);
    if (hexLen % 2 != 0)
    {
        return NULL; // Error: Invalid hexadecimal string length
    }
    
    size_t byteLen = hexLen / 2;
    if (byteLen > byteArraySize)
    {
        return NULL; // Error: Byte array too small
    }    

    for (size_t i = 0; i < byteLen; ++i)
    {
        if (sscanf(&hexString[i * 2], "%2hhx", &byteArray[i]) != 1)
        {
            return NULL; // Error: Failed to parse hexadecimal string
        }
    }

    return byteArray;
}

unsigned long long byteArrayToLongLong(const uint8_t *byteArray, size_t length) {
    unsigned long long result = 0;
    for (size_t i = 0; i < length; ++i) {
        result = (result << 8) | (unsigned long long)byteArray[i];
    }
    return result;
}

void longLongToByteArray(unsigned long long value, uint8_t *byteArray, size_t length) {
    for (size_t i = length - 1; i >= 0; --i) {
        byteArray[i] = value & 0xFF;
        value >>= 8;
    }
}





void printUsage() {
    printf("Usage: ./your_program -t <num_threads_hash> -o <num_threads_sort> -f <filename> -m <memorysize_GB> -s <filesize_GB>\n");
    printf("Usage: ./your_program -p <num_records> -f <filename>\n");
}

void printHelp() {
    printf("Help:\n");
    printf("  -t <num_threads_hash>: Specify the number of threads to generate hashes\n");
    printf("  -o <num_threads_sort>: Specify the number of threads to sort hashes\n");
    printf("  -f <filename>: Specify the filename\n");
    printf("  -m <memorysize>: Specify the memory size as an integer\n");
    printf("  -s <filesize>: Specify the filesize as an integer\n");
    printf("  -p <num_records>: Specify the number of records to print, must specify -f <filename>\n");
    printf("  -a <search_hash>: Specify the number of records to print, must specify -f <filename>\n");
    printf("  -l <prefix_length>: Specify the number of records to print, must specify -f <filename>\n");
    printf("  -c <search_records>: Specify the number of records to print, must specify -f <filename>\n");
    printf("  -d : turns on debug mode \n");
    printf("  -h, --help: Display this help message\n");
}

double min(double a, double b) {
    return (a < b) ? a : b;
}





int main(int argc, char *argv[]) 
{

	Timer timer;
    double elapsedTime;

//struct timeval start_all_walltime, end_all_walltime;

char *FILENAME = NULL; // Default value
long long FILESIZE = 0; // Default value
int num_threads_sort = 1;
long long memory_size = 0; //in GB
long long print_records = 0;

uint8_t byteArray[10];
uint8_t *targetHash = NULL;
size_t prefixLength = 10;

    int search_records = 0;



   int opt;
    while ((opt = getopt(argc, argv, "t:o:m:f:s:p:a:l:c:d:h")) != -1) {
        switch (opt) {
            case 't':
                NUM_THREADS = atoi(optarg);
                printf("NUM_THREADS=%d\n", NUM_THREADS);
                
                BATCH_SIZE = HASHGEN_THREADS_BUFFER/NUM_THREADS;
                printf("BATCH_SIZE=%ld\n", BATCH_SIZE);
                
                if (NUM_THREADS == 1)
				{
					printf("multi-threading with only 1 thread is not supported at this time, exiting\n");
					return 1;
		
        		}
                break;
            case 'o':
                num_threads_sort = atoi(optarg);
                printf("num_threads_sort=%d\n", num_threads_sort);
                if (num_threads_sort > 1)
				{
					printf("multi-threading for sorting has not been implemented, exiting\n");
					return 1;
		
        		}

                break;
            case 'm':
                memory_size = atoi(optarg);
                printf("memory_size=%lld GB\n", memory_size);
                break;
            case 'f':
                FILENAME = optarg;
                printf("FILENAME=%s\n", FILENAME);
                break;
            case 's':
                FILESIZE = atoi(optarg);
                printf("FILESIZE=%lld GB\n", FILESIZE);
                break;
            case 'p':
                print_records = atoi(optarg);
                printf("print_records=%lld\n", print_records);
                break;
            case 'a':
            	// Validate the length of the hash
    			if (strlen(optarg) != 20) // Each byte represented by 2 characters in hexadecimal
    			{
        			printf("Invalid hexadecimal hash format\n");
        			return 1;
   				}
			    // Convert the hexadecimal hash from command-line argument to binary
			    
			    targetHash = hexStringToByteArray(optarg, byteArray, sizeof(byteArray));
			    if (targetHash == NULL)
			    {
			        printf("Error: Byte array too small\n");
			        return 1;
			    }
    
				
				printf("Hash_search=");
        		printBytes(byteArray, sizeof(byteArray));    
        		printf("\n");
   				
                
                //printf("print_records=%lld\n", print_records);
                break;
            case 'l':
    // Get the length of the prefix
    prefixLength = atoi(optarg);
    if (prefixLength <= 0 || prefixLength > 10)
    {
        printf("Invalid prefix length\n");
        return 1;
    }
                printf("prefixLength=%zu\n", prefixLength);
                break;
            case 'c':
    // Get the length of the prefix
    search_records = atoi(optarg);
    if (search_records <= 0)
    {
        printf("Invalid search records\n");
        return 1;
    }
                printf("search_records=%d\n", search_records);
                break;            
            case 'd':
if (strcmp(optarg, "true") == 0) {
        DEBUG = true;
    } else if (strcmp(optarg, "false") == 0) {
        DEBUG = false;
    } else {
    DEBUG = false;
    }            
                printf("DEBUG=%s\n",DEBUG ? "true" : "false");
                break;
            case 'h':
                printHelp();
                return 0;
            default:
                printUsage();
                return 1;
        }
    } 
    
    
    
    
    if (FILENAME == NULL) {
        printf("Error: filename (-f) is mandatory.\n");
        printUsage();
        return 1;
    }
	if (FILENAME != NULL && print_records == 0 && targetHash == NULL && search_records == 0 && (NUM_THREADS <= 0 || num_threads_sort <=0 || FILESIZE <= 0 || memory_size <= 0)) 
	{
        printf("Error: mandatory command line arguments have not been used, try -h for more help\n");
        printUsage();
        return 1;
    }    
    
                //NUM_THREADS = atoi(optarg);
                printf("NUM_THREADS=%d\n", NUM_THREADS);
                
                BATCH_SIZE = HASHGEN_THREADS_BUFFER/NUM_THREADS;
                printf("BATCH_SIZE=%ld\n", BATCH_SIZE);    
    
    
    
    //print records
    if (print_records > 0) 
    {
    // Print the contents of the specified file
    	printf("Printing first %lld of file '%s'...\n", print_records,FILENAME);
    	printFile(FILENAME, print_records);
        return 0;
    }
    //search hash
    else if (targetHash != NULL)
    {
    // Open file for reading
    int fd = open(FILENAME, O_RDONLY);
    if (fd < 0)
    {
        perror("Error opening file for reading");
        return 1;
    }
    long long filesize = getFileSize(FILENAME);

    // Timing variables
    //struct timespec start, end;
    //double elapsedTime;

	

    // Get start time
  //  clock_gettime(CLOCK_MONOTONIC, &start);
  //  gettimeofday(&start_all_walltime, NULL);
  	resetTimer(&timer);

    // Perform binary search
    int seekCount = 0;
    int index = binarySearch(targetHash, prefixLength, fd, filesize, &seekCount, false);

    // Get end time
    //clock_gettime(CLOCK_MONOTONIC, &end);

    // Calculate elapsed time in microseconds
    //elapsedTime = (end.tv_sec - start.tv_sec) * 1e6 + (end.tv_nsec - start.tv_nsec) / 1e3;
    elapsedTime = getTimer(&timer);

    if (index >= 0)
    {
        // Hash found
        MemoRecord foundNumber;
        lseek(fd, index * sizeof(MemoRecord), SEEK_SET);
        read(fd, &foundNumber, sizeof(MemoRecord));

        printf("Hash found at index: %d\n", index);
        printf("Number of lookups: %d\n", seekCount);
        printf("Hash search: ");
        printBytes(byteArray, sizeof(byteArray));
        printf("/%zu\n",prefixLength);
        //printf("Prefix length: %zu\n", prefixLength);
        printf("Hash found : ");
        printBytes(foundNumber.hash, sizeof(foundNumber.hash));
        printf("\n");

        
        unsigned long long nonceValue = 0;
        for (int i = 0; i < sizeof(foundNumber.nonce); i++) {
            nonceValue |= (unsigned long long)foundNumber.nonce[i] << (i * 8);
        }

        printf("Nonce: %llu/", nonceValue);
        //printf("Nonce (hex): ");
        printBytes(foundNumber.nonce, sizeof(foundNumber.nonce));
        printf("\n");

        
    }
    else
    {
        printf("Hash not found after %d lookups\n",seekCount);
    }
    printf("Time taken: %.0f microseconds\n", elapsedTime*1000000.0);

    // Close the file
    close(fd);

    return 0;
        }
    //search bulk
    else if (search_records > 0)
    {
    	printf("searching for random hashes\n");
	size_t numberLookups = search_records;
    if (numberLookups <= 0)
    {
        printf("Invalid number of lookups\n");
        return 1;
    }

    // Get the length of the prefix
    //size_t prefixLength = atoi(argv[2]);
    if (prefixLength <= 0 || prefixLength > 10)
    {
        printf("Invalid prefix length\n");
        return 1;
    }

    // Open file for reading
    int fd = open(FILENAME, O_RDONLY);
    if (fd < 0)
    {
        perror("Error opening file for reading");
        return 1;
    }
    long filesize = getFileSize(FILENAME);

// Seed the random number generator with the current time
    srand((unsigned int)time(NULL));
	int seekCount = 0;
	int found = 0;
	int notfound = 0;
	
    // Timing variables
    //struct timespec start, end;
    //double elapsedTime;

    // Get start time
    //clock_gettime(CLOCK_MONOTONIC, &start);
    resetTimer(&timer);

	for (int searchNum = 0;searchNum<numberLookups;searchNum++)
	{

    // Convert the hexadecimal hash from command-line argument to binary
    //char hexString[length * 2 + 1];
    
    uint8_t byteArray[HASH_SIZE];
    for (size_t i = 0; i < HASH_SIZE; ++i) {
    	byteArray[i] = rand() % 256;
    }
    
    uint8_t *targetHash = byteArray;
    if (targetHash == NULL)
    {
        printf("Error: Byte array too small\n");
        return 1;
    }

    // Perform binary search
    int index = binarySearch(targetHash, prefixLength, fd, filesize, &seekCount,true);

    if (index >= 0)
    {
        // Hash found
        MemoRecord foundNumber;
        lseek(fd, index * sizeof(MemoRecord), SEEK_SET);
        read(fd, &foundNumber, sizeof(MemoRecord));

		found++;
        if (DEBUG) 
        {
        printf("Hash found at index: %d\n", index);
        printf("Hash found : ");
        printBytes(foundNumber.hash, sizeof(foundNumber.hash));
        printf("\n");
        
        unsigned long long nonceValue = 0;
        for (int i = 0; i < sizeof(foundNumber.nonce); i++) {
            nonceValue |= (unsigned long long)foundNumber.nonce[i] << (i * 8);
        }

        printf("Nonce: %llu/", nonceValue);
        //printf("Nonce (hex): ");
        printBytes(foundNumber.nonce, sizeof(foundNumber.nonce));
        printf("\n");
        }

    }
    else
    {
    	notfound++;
		if (DEBUG) 
			printf("Hash not found\n");
    }
    
    
    	if (DEBUG)
    	{
        printf("Number of lookups: %d\n", seekCount);
        printf("Hash search: ");
        printBytes(byteArray, sizeof(byteArray));
        printf("/%zu\n",prefixLength);
        //printf("Prefix length: %zu\n", prefixLength);
    	}
    
    }
    
        // Get end time
    //clock_gettime(CLOCK_MONOTONIC, &end);

    // Calculate elapsed time in microseconds
    //elapsedTime = (end.tv_sec - start.tv_sec) * 1e6 + (end.tv_nsec - start.tv_nsec) / 1e3;
	elapsedTime = getTimer(&timer);
        printf("Number of total lookups: %zu\n", numberLookups);
        printf("Number of searches found: %d\n", found);
        printf("Number of searches not found: %d\n", notfound);
        printf("Number of total seeks: %d\n", seekCount);
        printf("Time taken: %.2f ms/lookup\n", elapsedTime*1000.0/numberLookups);
        printf("Throughput lookups/sec: %.2f\n", numberLookups/elapsedTime);

    // Close the file
    close(fd);

    return 0;
        }
    //hash generation
    else
    {
    	printf("hash generation and sorting...\n");

	NUM_ENTRIES = FILESIZE*1024*1024*1024/RECORD_SIZE;
    printf("NUM_ENTRIES=%lld\n", NUM_ENTRIES);
	
	BUCKET_SIZE = (memory_size*1.0*1024*1024*1024)/(RECORD_SIZE*NUM_BUCKETS);
	//BUCKET_SIZE = 1024;
    printf("BUCKET_SIZE=%d\n", BUCKET_SIZE);

    printf("NUM_BUCKETS=%d\n", NUM_BUCKETS);

    long long EXPECTED_TOTAL_FLUSHES = ((long long)(NUM_ENTRIES))/BUCKET_SIZE;
    printf("EXPECTED_TOTAL_FLUSHES=%llu\n", EXPECTED_TOTAL_FLUSHES);
    
    //long long ONE = 1;
	FLUSH_SIZE = (int)EXPECTED_TOTAL_FLUSHES/(int)NUM_BUCKETS;
	//double temp = (double)EXPECTED_TOTAL_FLUSHES/NUM_BUCKETS;
	//BUCKET_SIZE = 1024;
	//int temp = 262144/65536;
    printf("FLUSH_SIZE=%llu\n", FLUSH_SIZE);
    
	long long MEMORY_MAX = (long long)NUM_BUCKETS * BUCKET_SIZE * RECORD_SIZE;
    printf("MEMORY_MAX=%lld\n", MEMORY_MAX);
    printf("RECORD_SIZE=%d\n", RECORD_SIZE);
    printf("HASH_SIZE=%d\n", RECORD_SIZE - NONCE_SIZE);
    printf("NONCE_SIZE=%d\n", NONCE_SIZE);
    
    // Open file for writing
    int fd = open(FILENAME, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("Error opening file for writing");
        return 1;
    }
    
    resetTimer(&timer);
    

    // Array to hold the generated random records
    MemoRecord record;
// Allocate memory for the array of Bucket structs
Bucket *buckets = malloc(NUM_BUCKETS * sizeof(Bucket));
if (buckets == NULL) {
    perror("Error allocating memory");
    return 1;
}

// Initialize each bucket
for (int i = 0; i < NUM_BUCKETS; ++i) {
    // Allocate memory for the records in each bucket
    buckets[i].records = malloc(BUCKET_SIZE * sizeof(MemoRecord));
    if (buckets[i].records == NULL) {
        perror("Error allocating memory");
        // Free previously allocated memory
        for (int j = 0; j < i; ++j) {
            free(buckets[j].records);
        }
        free(buckets);
        return 1;
    }
    buckets[i].count = 0; // Initialize the record count for each bucket
    buckets[i].flush = 0; // Initialize flush for each bucket
    
}


    printf("initializing circular array...\n");
    // Initialize the circular array
    struct CircularArray circularArray;
    initCircularArray(&circularArray);

    printf("Create thread data structure...\n");
    // Create thread data structure
    struct ThreadData threadData[NUM_THREADS];

    printf("Create hash generation threads...\n");
    // Create threads
    pthread_t threads[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
        threadData[i].circularArray = &circularArray;
        threadData[i].threadID = i;
        pthread_create(&threads[i], NULL, arrayGenerationThread, &threadData[i]);
    }
	printf("Hash generation threads created...\n");

	unsigned long long totalFlushes = 0;

    // Measure time taken for hash generation using gettimeofday
    //struct timeval start_time, end_time, last_progress_time;
    //gettimeofday(&start_time, NULL);
    //gettimeofday(&last_progress_time, NULL);
    long long last_progress_i = 0;
    //double elapsed_time = 0.0;

	elapsedTime = getTimer(&timer);
	double last_progress_update = elapsedTime;

    // Start timing
    //clock_t start = clock();

    // Write random records to buckets based on their first 2-byte prefix
    unsigned long long i = 0;
    int flushedBucketsNeeded = NUM_BUCKETS;
    MemoRecord consumedArray[BATCH_SIZE];
    
    while (flushedBucketsNeeded > 0)
    //for (unsigned long long i = 0; i < NUM_ENTRIES; i++) 
    {
    
        
        
        if (DEBUG)
        printf("removeBatch()...\n");
        removeBatch(&circularArray, consumedArray);

        if (DEBUG)
        printf("processing batch of size %ld...\n",BATCH_SIZE);
		for (int b = 0; b<BATCH_SIZE; b++)
		{    
    
        // Generate a random record
        //generateBlake3(&record, i + 1);

        // Calculate the bucket index based on the first 2-byte prefix
        //int bucketIndex = (record.hash[0] << 8) | record.hash[1];
        
        off_t bucketIndex = getBucketIndex(consumedArray[b].hash);

        // Add the random record to the corresponding bucket
        Bucket *bucket = &buckets[bucketIndex];
        if (bucket->count < BUCKET_SIZE) 
        {
            bucket->records[bucket->count++] = consumedArray[b];
        } 
        else 
        {
            // Bucket is full, write it to disk
            if (bucket->flush < FLUSH_SIZE)
            {
            //if all buckets fit in memory, sort them now before writing to disk
            if (FLUSH_SIZE == 1)
            {
            	// Sort the bucket contents
        		//printf("");
        		qsort(bucket->records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords);
            }
            
            writeBucketToDisk(bucket, fd, bucketIndex);

            // Reset the bucket count
            bucket->count = 0;
            bucket->flush++;
            if (bucket->flush == FLUSH_SIZE)
            	flushedBucketsNeeded--;
            
            totalFlushes++;
            }
            
        }
        }
        
        
        
            //gettimeofday(&end_time, NULL);
            elapsedTime = getTimer(&timer);
            //elapsed_time = (end_time.tv_sec - start_time.tv_sec) +
            //                      (end_time.tv_usec - start_time.tv_usec) / 1.0e6;


        if (elapsedTime > last_progress_update + 1.0) {
        	double elapsed_time_since_last_progress_update = elapsedTime - last_progress_update;
        	last_progress_update = elapsedTime;
        	
        	
            // Calculate and print estimated completion time
            double progress = min(i*100.0/NUM_ENTRIES,100.0);
            double remaining_time = elapsedTime / (progress / 100.0) - elapsedTime;
            
            
            printf("[%.0lf][HASHGEN]: %.2lf%% completed, ETA %.1lf seconds, %llu/%llu flushes, %.1lf MB/sec\n", floor(elapsedTime), progress, remaining_time,totalFlushes,EXPECTED_TOTAL_FLUSHES, ((i-last_progress_i) / elapsed_time_since_last_progress_update)* (8.0 + 8.0) / (1024 * 1024));

            last_progress_i = i;
            //gettimeofday(&last_progress_time, NULL);
        }
        //i++;
        i += BATCH_SIZE;
         
    }
    
    
    //gettimeofday(&end_time, NULL);
    //        double elapsed_time_hashgen = (end_time.tv_sec - start_time.tv_sec) +
    //                              (end_time.tv_usec - start_time.tv_usec) / 1.0e6;
    
    if (DEBUG)
    printf("finished generating %llu hashes and wrote them to disk using %llu flushes...\n",i,totalFlushes);
   
   
  
    bool doSort = true;

    elapsedTime = getTimer(&timer);
    last_progress_update = elapsedTime;
    double sort_start = elapsedTime;

    
    if (FLUSH_SIZE > 1)
    {
    //double reading_time, sorting_time, writing_time, total_time;
    //struct timeval start_time, end_time, start_all, end_all;
    //long long elapsed;
	int EXPECTED_BUCKETS_SORTED = NUM_BUCKETS;
    //gettimeofday(&start_all, NULL);
    

    // Allocate memory for a single bucket
    Bucket bucket;
    bucket.count = BUCKET_SIZE;
    bucket.records = malloc(BUCKET_SIZE * FLUSH_SIZE * sizeof(MemoRecord));
    if (bucket.records == NULL) {
        perror("Memory allocation failed");
        close(fd);
        return 1;
    }

	last_progress_i = 0;
    // Read each bucket from the file, sort its contents, and write it back to the file
    //not sure why i < NUM_BUCKETS-1 is needed
    for (unsigned long long i = 0; i < NUM_BUCKETS - 1; i++) {
    	//printf("processing bucket i=%d\n",i);
        // Seek to the beginning of the bucket
        if (lseek(fd, i * sizeof(MemoRecord) * BUCKET_SIZE * FLUSH_SIZE, SEEK_SET) < 0) {
            printf("main(): Error seeking in file at offset %llu\n",i * sizeof(MemoRecord) * BUCKET_SIZE * FLUSH_SIZE);
            close(fd);
            return 1;
        }

        // Read the bucket into memory
        ssize_t bytesRead = read(fd, bucket.records, BUCKET_SIZE * FLUSH_SIZE * sizeof(MemoRecord));
        if (bytesRead < 0 || (size_t)bytesRead != BUCKET_SIZE * FLUSH_SIZE * sizeof(MemoRecord)) {
            printf("Error reading bucket %llu from file\n",i);
            close(fd);
            return 1;
        }
        //else
        //{

		if (num_threads_sort > 1)
		{
			printf("multi-threading for sorting has not been implemented, exiting\n");
			return 1;
		
        }
        else
        {

        // Sort the bucket contents
        	qsort(bucket.records, BUCKET_SIZE, sizeof(MemoRecord), compareMemoRecords);
        }

        // Write the sorted bucket back to the file
        if (lseek(fd, i * sizeof(MemoRecord) * BUCKET_SIZE * FLUSH_SIZE, SEEK_SET) < 0) {
            printf("main(2): Error seeking in file at offset %llu\n",i * sizeof(MemoRecord) * BUCKET_SIZE * FLUSH_SIZE);
            close(fd);
            return 1;
        }
        if (write(fd, bucket.records, BUCKET_SIZE * FLUSH_SIZE * sizeof(MemoRecord)) < 0) {
            perror("Error writing bucket to file");
            close(fd);
            return 1;
        }
        
        //gettimeofday(&end_time, NULL);
        //elapsed = (end_time.tv_sec - start_time.tv_sec) * 1000000LL +
        //                     (end_time.tv_usec - start_time.tv_usec);

		elapsedTime = getTimer(&timer);
		
        //writing_time += elapsedTime / 1000000.0;
//gettimeofday(&end_all, NULL);
    //elapsed = (double)((end_all.tv_sec - start_all.tv_sec) * 1000000LL +
      //                      (end_all.tv_usec - start_all.tv_usec))/ 1000000.0;

		//double offset = elapsed_time_hashgen - floor(elapsed_time_hashgen);

        if (elapsedTime > last_progress_update + 1) {
        double elapsed_time_since_last_progress_update = elapsedTime - last_progress_update;
        

	     	int totalBucketsSorted = i;//i*NUM_THREADS; 
		float perc_done = totalBucketsSorted*100.0/EXPECTED_BUCKETS_SORTED;
              float eta = (elapsedTime-sort_start) / (perc_done/100.0) - (elapsedTime-sort_start);
                float diskSize = NUM_BUCKETS * BUCKET_SIZE * FLUSH_SIZE * sizeof(MemoRecord) / (1024 * 1024);
	    float throughput_MB = diskSize*perc_done*1.0/100.0/(elapsedTime-sort_start);
	    float throughput_MB_latest = (((i-last_progress_i)*BUCKET_SIZE * FLUSH_SIZE * sizeof(MemoRecord)*1.0)/(elapsedTime-last_progress_update))/(1024 * 1024);
	    if (DEBUG)
	    printf("%llu %llu %d %llu %lu %f %f\n",i,last_progress_i,BUCKET_SIZE,FLUSH_SIZE,sizeof(MemoRecord),elapsedTime,last_progress_update);
	    float progress = perc_done;
	    //double elapsed_time = elapsed;
	    float remaining_time = eta;
		//printf("Buckets sorted : %d in %lld sec %.2f%% ETA %lf sec => %.2f MB/sec\n", i*NUM_THREADS, elapsed, perc_done, eta, throughput_MB);
            printf("[%.0lf][SORT]: %.2lf%% completed, ETA %.1lf seconds, %llu/%d flushes, %.1lf MB/sec\n", elapsedTime, progress, remaining_time,i,NUM_BUCKETS, throughput_MB_latest);

	last_progress_i = i;
	        	
	        	last_progress_update = elapsedTime;

            //gettimeofday(&last_progress_time, NULL);
		//printf("Buckets sorted : %zu\n", i*NUM_THREADS);
	}        
    }

    // Free memory allocated for records in each bucket
    for (int i = 0; i < NUM_BUCKETS; i++) {
        free(buckets[i].records);
    }
    free(buckets);

    }

	

    close(fd);

    // End timing
    //clock_t end = clock();
    //gettimeofday(&end_all_walltime, NULL);
    
    elapsedTime = getTimer(&timer);
    
    //double elapsed_walltime = (end_all_walltime.tv_sec - start_all_walltime.tv_sec) +
    //                              (end_all_walltime.tv_usec - start_all_walltime.tv_usec) / 1.0e6;
    
    //long long elapsed_walltime = (end_all_walltime.tv_sec - start_all_walltime.tv_sec) * 1000000LL +
    //                         (end_all_walltime.tv_usec - start_all_walltime.tv_usec);
	//double elapsed_walltime = elapsed_time+elapsed_time_hashgen;

    // Calculate elapsed time in seconds
    //double elapsed_seconds = (double)(end - start) / CLOCKS_PER_SEC;

    // Calculate hashes per second
    double hashes_per_second = NUM_ENTRIES / elapsedTime;

    // Calculate bytes per second
    double bytes_per_second = sizeof(MemoRecord) * NUM_ENTRIES / elapsedTime;

    printf("Completed %lld GB vault %s in %.2lf seconds : %.2f MH/s %.2f MB/s\n", FILESIZE, FILENAME, elapsedTime, hashes_per_second/1000000.0, bytes_per_second*1.0/(1024*1024));
    //printf("MH/s: %.2f\n", );
    //printf("MB/s: %.2f\n", );

    return 0;
    }
}