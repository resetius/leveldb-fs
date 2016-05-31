#pragma once

#include <fuse/fuse.h>

#include <vector>
#include <boost/unordered_set.hpp>
#include <boost/shared_ptr.hpp>

#include "dentry.h"

struct FS
{
	int maxhandles;
	int blocksize;
	leveldb::DB* db;

	std::vector<boost::shared_ptr<entry> > handles;
	boost::unordered_set<uint64_t> allocated_handles;

	boost::shared_ptr<dentry> root;

	boost::shared_ptr<entry> find(const std::string & path);
	boost::shared_ptr<entry> find_parent(const std::string & path);
	boost::shared_ptr<entry> find_handle(uint64_t t);
	std::string filename(const std::string & path);

	uint64_t allocate_handle(const boost::shared_ptr<entry> & r, struct fuse_file_info *fi);
	void release_handle(uint64_t h);
	

	leveldb::Status write(leveldb::WriteBatch & batch, bool sync = false);

	void mkfs();
	void mount();

	FS();
};
