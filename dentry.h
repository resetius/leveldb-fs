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
#include <boost/log/common.hpp>

#include <list>

struct dentry;
struct entry;
struct FS;

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

	block_key(const block_key & other)
	{
		type = other.type;
		blockno = other.blockno;
		meta = other.meta;
		memcpy(inode, other.inode, sizeof(inode));
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

	bool operator < (const block_key & other) const {
		if (type < other.type) {
			return true;
		} else if (type > other.type) {
			return false;
		} else {
			int r = memcmp(inode, other.inode, sizeof(inode));
			if (r < 0) {
				return true;
			} else if (r > 0) {
				return false;
			} else if (blockno < other.blockno) {
				return true;
			} else {
				return false;
			}
		}
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
	FS * fs;
	boost::log::sources::severity_logger< >& lg;

	boost::mutex mutex;
	struct stat st;

	uuid_t inode;
	char type;
	std::string name;
	std::string target_name; // for symlink
	entries_t entries;

	entry(const std::string & name, FS * fs);
	virtual ~entry() {}
	virtual bool read();
	virtual void write(batch_t & batch);

	boost::shared_ptr<entry> find(const std::string & path);

	virtual void fillstat(struct stat * s);
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

	virtual void remove(batch_t & batch) {}
	virtual void truncate(batch_t & batch, size_t new_size) {}

	void add_child(const boost::shared_ptr<entry> & e);
	void remove_child(const std::string & name);
	void remove_child(const boost::shared_ptr<entry> & e);

	static std::string stringify(const std::string & key);

	std::string tostring() {
		std::string r = name + ";";
		char buf[1024];
		uuid_unparse(inode, buf);
		r += buf;
		return r;
	}
};

struct fentry: public entry {
	fentry(const std::string & name, FS * fs);
	
	int write_buf(batch_t & batch,
	              const char * buf,
	              off_t size,
	              size_t offset);

	int read_buf(char * buf,
	             off_t size, size_t offset);

	void remove(batch_t & batch);
	void truncate(batch_t & batch, size_t new_size);
	void grow(batch_t & batch, size_t new_size);
};

struct dentry: public entry {
	dentry(const std::string & name, FS * fs);
	void remove(batch_t & batch);
};

struct symlink_entry: public dentry {	
	symlink_entry(const std::string & name, FS * fs);
};
