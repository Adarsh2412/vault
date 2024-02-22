CC = gcc
CFLAGS = -lblake3 -lpthread -O3 
#MacOS
#LDFLAGS = -I/opt/homebrew/opt/blake3/include -L/opt/homebrew/opt/blake3/lib
#Raspberry Pi
#LDFLAGS = -pg -DBLAKE3_USE_NEON=1 -lpthread -lm
#Ubuntu
LDFLAGS = -pg -lpthread -lm


EXECUTABLE = vault
#MacOS
#SRCC = vault.c
#Raspberry Pi
#SRCC = vault.c blake3.c blake3_dispatch.c     blake3_portable.c blake3_neon.c
#Ubuntu
SRCC = vault.c blake3.c blake3_dispatch.c blake3_portable.c blake3_sse2_x86-64_unix.S blake3_sse41_x86-64_unix.S blake3_avx2_x86-64_unix.S blake3_avx512_x86-64_unix.S

all: 
	$(CC) -o $(EXECUTABLE) ${SRCC} $(CFLAGS) $(LDFLAGS)

clean:
	rm -f $(EXECUTABLE)

