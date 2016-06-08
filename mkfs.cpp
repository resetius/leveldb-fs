#include "fs.h"

int main(int argc, char ** argv)
{
	if (argc < 2) {
		fprintf(stderr, "set db first\n");
		return -1;
	}
	FS * fs = new FS(argv[1]);
	fs->mkfs();
	delete fs;
	return 0;
}
