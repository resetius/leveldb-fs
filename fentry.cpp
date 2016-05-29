#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "dentry.h"

#include <arpa/inet.h>

extern leveldb::DB* db;
extern FILE * l;
extern int blocksize;

int fentry::write_buf(leveldb::WriteBatch & batch,
                      const char * buf,
                      off_t size, size_t offset)
{
	int cur_block  = offset / blocksize;
	int cur_offset = offset;
	int write_size = 0;
	const char * p = buf;

	fprintf(l, "cur offset %s %d\n", name.c_str(), cur_offset);
	int upto = 1;

	int r = offset % blocksize;
	leveldb::ReadOptions options;

	std::string base = key();

	if (r != 0) {
		std::string key = base;
		int tmp = htonl(cur_block);
		key.insert(key.end(), (char*)&tmp, (char*)&tmp + sizeof(tmp));
		
		std::string value;
		leveldb::Status st = db->Get(options, key, &value);

		value.resize(blocksize);

		char * dst = (char*)value.c_str() + r;
		upto = std::min((long)(blocksize - r), (long)size);
		memcpy(dst, p, upto);
		value.resize(r+upto);

		fprintf(l, "write(1)key %s\n", key.c_str());
		batch.Put(key, value);
		write_size += upto;

		p +=  upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;

		if (upto != blocksize - r) {
			upto = 0;
		}
	}

	while (upto > 0) {
		std::string key = base;
		int tmp = htonl(cur_block);
		key.insert(key.end(), (char*)&tmp, (char*)&tmp + sizeof(tmp));

		std::string value;
		upto = std::min(buf + size - p, (long)blocksize);
		if (upto == 0) {
			break;
		}
		fprintf(l, "write key %s\n", key.c_str());
		batch.Put(key, leveldb::Slice(p, upto));
		write_size += upto;
		p += upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;
	}

	int filesize = std::max((long)this->size, (long)(offset+size));

	write(batch);

	this->size = filesize;

	fprintf(l, "written %s %d\n", name.c_str(), write_size);

	return write_size;
}

int fentry::read_buf(char * buf,
                     off_t size, size_t offset)
{
	int cur_block  = offset / blocksize;
	int cur_offset = offset;
	int read_size = 0;
	char * p = buf;
	leveldb::ReadOptions options;

	std::string base = key();

	fprintf(l, "read %s <- %lu, %lu %lu\n",
	        name.c_str(), this->size, size, offset);

	while (cur_offset < this->size) {
		std::string key = base;
		int tmp = htonl(cur_block);
		key.insert(key.end(), (char*)&tmp, (char*)&tmp + sizeof(tmp));

		std::string value;
		leveldb::Status st = db->Get(options, key, &value);
		if (!st.ok()) {
			fprintf(l, "cannot read key %s\n", key.c_str());
			break;
		}
		fprintf(l, "read key %s\n", key.c_str());
		int upto = std::min(buf + size - p, (long)value.size());
		read_size += upto;
		memcpy(p, value.c_str() + cur_offset % blocksize, value.size());
		p += upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;
	}

	fprintf(l, "read done %s -> %d\n", name.c_str(), read_size);

	return read_size;
}


