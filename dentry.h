#pragma once

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include <sys/stat.h>
#include <arpa/inet.h>

#include <string>
#include <uuid/uuid.h>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/thread/mutex.hpp>
#include <list>

struct dentry;
struct entry;

typedef boost::unordered_map<std::string, boost::shared_ptr<entry> > entries_t;

#pragma pack (push, 1)
struct block_key
{
	char type;
	uuid_t inode;
	int blockno;

	bool meta;

	int size() const {
		if (!meta) {
			return sizeof(type)+sizeof(inode)+sizeof(blockno);
		} else {
			return sizeof(type)+sizeof(inode);
		}
	}

	std::string tostring() const {
		std::string r;
		r += type;
		r += ";";
		char buf[1024];
		uuid_unparse(inode, buf);
		r += buf;
		r += ";";
		snprintf(buf, sizeof(buf), "%04d", ntohl(blockno));
		r += buf;
		return r;
	}

	block_key(char type, uuid_t ino): type(type), blockno(-1), meta(true)
	{
		memcpy(inode, ino, sizeof(inode));
	}

	block_key(char type, uuid_t ino, int blockno):
		type(type),
		blockno(0),
		meta(false)
	{
		memcpy(inode, ino, sizeof(inode));
	}

	void setblock(int block) {
		blockno = htonl(block);
		meta = false;
	}
};
#pragma pack ( pop)

struct operation
{
	enum {
		PUT = 0,
		DELETE = 1
	};

	block_key key;
	int type;
	std::string data;

	operation(const block_key & key, int type, const std::string & data):
		key(key), type(type), data(data)
	{
	}
};

typedef std::vector<operation> batch_t;

struct entry: public boost::enable_shared_from_this<entry> {
	boost::mutex mutex;
	struct stat st;

	uuid_t inode;
	std::string name;
	entries_t entries;

	entry(const std::string & name);
	virtual ~entry() {}
	virtual bool read();
	virtual void write(batch_t & batch);

	boost::shared_ptr<entry> find(const std::string & path);

	virtual void fillstat(struct stat * s) = 0;
	virtual int write_buf(batch_t & batch,
	                      const char * buf,
	                      off_t size, size_t offset)
	{
		return 0;
	}

	virtual int read_buf(char * buf,
	                     off_t size, size_t offset)
	{
		return 0;
	}

	virtual char type() = 0;

	virtual void remove(batch_t & batch) {}
	virtual void truncate(batch_t & batch, size_t new_size) {}

	static std::string stringify(const std::string & key);
};

struct fentry: public entry {
	fentry(const std::string & name);
	
	int write_buf(batch_t & batch,
	              const char * buf,
	              off_t size,
	              size_t offset);

	int read_buf(char * buf,
	             off_t size, size_t offset);

	void remove(batch_t & batch);
	void truncate(batch_t & batch, size_t new_size);

	char type() {
		return 'f';
	}

	void fillstat(struct stat * s);
};

struct dentry: public entry {
	dentry(const std::string & name);
	void fillstat(struct stat * s);
	void remove(batch_t & batch);
	char type() {
		return 'd';
	}
};
