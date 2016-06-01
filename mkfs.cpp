#include "fs.h"

extern FS * fs;

int main(int argc, char ** argv)
{
	FS * f = new FS(argv[1], argv[2]);
	fs = f; //TODO: ugly
	f->mkfs();
	delete fs;
	return 0;
}
