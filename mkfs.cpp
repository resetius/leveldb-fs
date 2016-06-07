#include "fs.h"

int main(int argc, char ** argv)
{
	FS * fs = new FS(argv[1], argv[2]);
	fs->mkfs();
	delete fs;
	return 0;
}
