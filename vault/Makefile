CC = gcc
CFLAGS = -O3 
LDFLAGS = -DBLAKE3_USE_NEON=1 -lpthread -lm 

EXECUTABLE = vault
#SRCC = vault.c blake3.c blake3_dispatch.c blake3_portable.c blake3_sse2_x86-64_unix.S blake3_sse41_x86-64_unix.S blake3_avx2_x86-64_unix.S blake3_avx512_x86-64_unix.S
SRCC = vault.c blake3.c blake3_dispatch.c     blake3_portable.c blake3_neon.c
all: 
        $(CC) -o $(EXECUTABLE) ${SRCC} $(CFLAGS) $(LDFLAGS)

clean:
        rm -f $(EXECUTABLE)