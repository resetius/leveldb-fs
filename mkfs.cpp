#include <string.h>
#include "fs.h"

int main(int argc, char ** argv)
{
	if (argc < 2) {
		fprintf(stderr, "set db first\n");
		return -1;
	}

	const char * dbpath = argv[argc - 1];
	int blocksize = 128*1024;
	int parts = 2;

	for (int i = 1; i < argc - 1; ++i) {
		if (!strcmp(argv[i], "--blocksize")) {
			blocksize = atoi(argv[i+1]);
		} else if (!strcmp(argv[i], "--parts")) {
			parts = atoi(argv[i+1]);
		}
	}

	if (blocksize <= 0) {
		fprintf(stderr, "invalid blocksize %d\n", blocksize);
		return -1;
	}

	if (parts <= 0) {
		fprintf(stderr, "invalid parts %d\n", parts);
		return -1;
	}

	FS * fs = new FS(argv[1]);
	fs->mkfs(blocksize, parts);
	delete fs;
	return 0;
}

