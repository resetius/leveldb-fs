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
	uuid_t inode;
	std::string name;
	entries_t entries;

	entry(const std::string & name): name(name) {}
	virtual ~entry() {}
	virtual bool read() = 0;
	virtual void write(leveldb::WriteBatch & batch) = 0;
	virtual std::string row();
	virtual std::string key() = 0;

	boost::shared_ptr<entry> find(const std::string & path);

	virtual void fillstat(struct stat * s) = 0;
};

struct fentry: public entry {
	size_t size;

	fentry(const std::string & name): entry(name) {}
	
	bool read() {
		return false;
	}

	void write(leveldb::WriteBatch & batch);
	std::string key();
	void fillstat(struct stat * s);
};

struct dentry: public entry {
	dentry(const std::string & name);
	bool read();
	void write(leveldb::WriteBatch & batch);
	std::string key();
	void fillstat(struct stat * s);
};
