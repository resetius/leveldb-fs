#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int blocksize;
int metasync;
int preallocate;

int newfile(long number)
{
	char fn[256];
	snprintf(fn, sizeof(fn), "%05ld", number);

	unlink(fn);
	
	int fd = open(fn, O_TRUNC | O_WRONLY | O_CREAT, 0644);

	return fd;
}

void * writer(void * a)
{
	long number = (long)a;

	char data[blocksize];
 
	for (int j = 0; j < blocksize; ++j) {
		data[j] = (char)(rand() % 256);
	}

	size_t written = 0;

	int fd = newfile(number);
	size_t maxsize = 128*1024*1024;
	if (preallocate) {
		fallocate(fd, 0, 0, maxsize);
	}

	while (1) {
		written += write(fd, data, sizeof(data));
		if (metasync) {
			fsync(fd);
		} else {
			fdatasync(fd);
		}

		if (written > maxsize) {
			close(fd);
			fd = newfile(number);
			written = 0;

			fprintf(stderr, "[%ld]: new segment\n", number); 
		}
	}

	close(fd);
}

int main(int argc, char ** argv)
{
	int threads = atoi(argv[1]);
	blocksize = atoi(argv[2]);
	metasync = atoi(argv[3]);
	preallocate = atoi(argv[4]);
	fprintf(stderr, "thread=%d, blocksize=%d, metasync=%d, preallocate=%d\n", 
			threads, blocksize, metasync, preallocate);

	pthread_t t[threads];

	for (long i = 0; i < threads; ++i) {
		pthread_create(&t[i], 0, writer, (void*)i);
	}

	for (int i = 0; i < threads; ++i) {
		pthread_join(t[i], 0);
	}

	return 0;
}
