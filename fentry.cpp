#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <arpa/inet.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "dentry.h"
#include "fs.h"

extern FILE * l;
extern FS * fs;

int fentry::write_buf(batch_t & batch,
                      const char * buf,
                      off_t size, size_t offset)
{

	int blocksize = fs->blocksize;

	int cur_block  = offset / blocksize;
	int cur_offset = offset;
	int write_size = 0;
	const char * p = buf;

//	fprintf(l, "cur offset %s %d\n", name.c_str(), cur_offset);
	int upto = 1;

	int r = offset % blocksize;

	block_key key(type(), inode, 0);

	if (r != 0) {
		key.setblock(cur_block);
		
		std::string value;
		fs->read(key, value); // TODO: check status

		value.resize(blocksize);

		char * dst = (char*)value.c_str() + r;
		upto = std::min((long)(blocksize - r), (long)size);
		memcpy(dst, p, upto);
		value.resize(r+upto);

//		fprintf(l, "write(1)key %s\n", stringify(key).c_str());
		batch.push_back(operation(key, operation::PUT, value));
		write_size += upto;

		p +=  upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;

		if (upto != blocksize - r) {
			upto = 0;
		}
	}

	while (upto > 0) {
		key.setblock(cur_block);
		
		upto = std::min(buf + size - p, (long)blocksize);
		if (upto == 0) {
			break;
		}
//		fprintf(l, "write key %s\n", stringify(key).c_str());

		std::string value(p, upto);
		batch.push_back(operation(key, operation::PUT, value));
		write_size += upto;
		p += upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;
	}

	int filesize = std::max((long)st.st_size, (long)(offset+size));

	if (st.st_size != filesize) {
		st.st_size = filesize;
		write(batch);
	}

//	fprintf(l, "written %s %d\n", name.c_str(), write_size);

	return write_size;
}

int fentry::read_buf(char * buf,
                     off_t size, size_t offset)
{
	int blocksize = fs->blocksize;

	int cur_block  = offset / blocksize;
	int cur_offset = offset;
	int read_size = 0;
	char * p = buf;

	block_key key(type(), inode, 0);

	st.st_atime = time(0);

	fprintf(l, "read %s <- %lu, %lu %lu\n",
	        name.c_str(), st.st_size, size, offset);

	while (cur_offset < st.st_size && cur_offset < offset+size) {
		key.setblock(cur_block);

		std::string value;
		bool status = fs->read(key, value); // TODO: check status

//		fprintf(l, " try key %s %d %d\n",
//		        stringify(key).c_str(),
//		        cur_offset, cur_block);

		if (!status) {
//			fprintf(l, "cannot read key %s -> %s\n",
//			        stringify(key).c_str(), status.ToString().c_str());
			break;
		}
//		fprintf(l, "read key %s %d %d\n",
//		        stringify(key).c_str(),
//		        cur_offset, cur_block);
		int upto = std::min(buf + size - p, (long)value.size());
		read_size += upto;
		memcpy(p, value.c_str() + cur_offset % blocksize, upto);
		p += upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;
	}

	fprintf(l, "read done %s -> %d\n", name.c_str(), read_size);

	return read_size;
}

void fentry::remove(batch_t & batch)
{
	int blocksize = fs->blocksize;

	size_t offset = 0;
	int cur_block  = offset / blocksize;
	int cur_offset = offset;

	block_key key(type(), inode, 0);

	while (cur_offset < st.st_size) {
		key.setblock(cur_block);

		batch.push_back(operation(key, operation::DELETE, std::string()));

		cur_block ++;
		cur_offset = cur_block * blocksize;
	}
}

void fentry::truncate(batch_t & batch, size_t new_size)
{
	int blocksize = fs->blocksize;

	if (new_size >= st.st_size) {
		return;
	}


	size_t offset = new_size;
	int cur_block  = offset / blocksize;
	int cur_offset = offset;

	// rewrite first block and delete last

	int r = offset % blocksize;

	block_key key(type(), inode, 0);

	if (r != 0) {
		key.setblock(cur_block);
		
		std::string value;
		//TODO: check status
		fs->read(key, value);

		value.resize(r);
		batch.push_back(operation(key, operation::PUT, value));

		cur_block ++;
		cur_offset = cur_block * blocksize;
	}

	while (cur_offset < st.st_size) {
		key.setblock(cur_block);

		batch.push_back(operation(key, operation::PUT, std::string()));

		cur_block ++;
		cur_offset = cur_block * blocksize;
	}

	st.st_size = new_size;
	write(batch);
}

