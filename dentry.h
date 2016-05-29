#pragma once

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include <sys/stat.h>

#include <string>
#include <uuid/uuid.h>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <list>

struct dentry;
struct entry;

typedef boost::unordered_map<std::string, boost::shared_ptr<entry> > entries_t;

struct entry: public boost::enable_shared_from_this<entry> {
	struct stat st;

	uuid_t inode;
	std::string name;
	entries_t entries;

	entry(const std::string & name);
	virtual ~entry() {}
	virtual bool read();
	virtual void write(leveldb::WriteBatch & batch);
	virtual std::string key() = 0;

	boost::shared_ptr<entry> find(const std::string & path);

	virtual void fillstat(struct stat * s) = 0;
	virtual int write_buf(leveldb::WriteBatch & batch,
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
};

struct fentry: public entry {
	fentry(const std::string & name);
	
	int write_buf(leveldb::WriteBatch & batch,
	              const char * buf,
	              off_t size,
	              size_t offset);

	int read_buf(char * buf,
	             off_t size, size_t offset);

	std::string key();
	void fillstat(struct stat * s);
};

struct dentry: public entry {
	dentry(const std::string & name);
	std::string key();
	void fillstat(struct stat * s);
};
