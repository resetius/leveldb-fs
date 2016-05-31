#include "fs.h"

int main(int argc, char ** argv)
{
	FS * fs = new FS();
	fs->mkfs();
	delete fs;
	return 0;
}
