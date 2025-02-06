You can build it with “make”.

You can run it like this:
./vault -t 16 -o 16 -i 16 -m 1 -s 64 -f vault.memo

You can print the first 10 hashes like this:
./vault -f vault.memo -p 10


You can lookup specific hashes like this:
./vault -f vault.memo -a 0000001aa672abb16aa1 -l 10


You can test many lookups like this:
./vault -f vault.memo -c 1000 -l 3


./vault -h will give you all the options. 

-t,-o,-i determine the number of threads (t: Number of threads used in hash generation, o: Number of threads used in hash sorting, i: Number of threads used in writing).

-m is memory size of RAM
-s file size in GB
-f file name
