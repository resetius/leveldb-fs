#pragma once

#include <boost/log/common.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>

#include <fuse/fuse.h>

#include <vector>
#include <boost/unordered_set.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <map>

#include "dentry.h"

BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(global_lg, boost::log::sources::severity_logger< >);

struct bucket
{
	size_t written;
	boost::mutex mutex;
	leveldb::DB * db;
	std::map<block_key, operation> batch;
	bool sync;
	bool read(const block_key & key, std::string & value);
	void add_op(const operation & op);
	bool flush(unsigned char * inode);
	bucket(): written(0) {}
};

struct FS
{
	boost::log::sources::severity_logger< >& lg;
	boost::mutex mutex;
	boost::thread flush_thread;

	int maxhandles;
	int blocksize;
	std::string dbroot;

	// opened files
	std::vector<boost::shared_ptr<entry> > handles;

	int parts;
	bucket * buckets;

	boost::unordered_set<uint64_t> allocated_handles;

	boost::shared_ptr<dentry> root;

	boost::shared_ptr<entry> find(const std::string & path);
	boost::shared_ptr<entry> find_parent(const std::string & path);
	boost::shared_ptr<entry> find_handle(uint64_t t);
	std::string filename(const std::string & path);

	uint64_t allocate_handle(const boost::shared_ptr<entry> & r, struct fuse_file_info *fi);
	void release_handle(uint64_t h);
	

	bool write(batch_t & batch, bool sync = false);
	bool read(const block_key & key, std::string & value);

	void mkfs(int blocksize, int parts);
	void mount();
	void open(bool create);

	int part(const block_key & key);
	bool sync(const boost::shared_ptr<entry> & e);


	void umount();
	void flush_buckets();
	void flush_job();

	FS(const std::string & dbpath);
};

