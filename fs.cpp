#include "leveldb/filter_policy.h"
#include "leveldb/env.h"

#include "fs.h"

FILE * l = 0;
FS * fs = 0;

FS::FS(const std::string & dbpath, const std::string & log)
{
	maxhandles=1000000;
	blocksize=4*1024;
	parts=4;

	l = fopen(log.c_str(), "w");
	setbuf(l, 0);


	fprintf(l, "init\n");

	dbroot = dbpath;

    handles.resize(maxhandles);

    buckets = new bucket[parts+1];
	root.reset(new dentry(""));
}

void FS::open(bool create)
{
	leveldb::Options options;
    options.create_if_missing = create;
    options.compression = leveldb::kNoCompression;
//    options.write_buffer_size = 32*1024*1024;

    options.filter_policy=leveldb::NewBloomFilterPolicy2(16);
    options.write_buffer_size=62914560;  // 60Mbytes
    options.total_leveldb_mem=2684354560; // 2.5Gbytes (details below)
    options.env=leveldb::Env::Default();
    
    leveldb::Status status;
    status = leveldb::DB::Open(options, dbroot + "/dentry", &buckets[0].db);
    
    for (int i = 0; i < parts; ++i) {
	    char buf[1024];
	    snprintf(buf, sizeof(buf), "/fentry-%04d", i);
	    status = leveldb::DB::Open(options, dbroot + buf, &buckets[i+1].db);
    }
}

void FS::mount()
{
	open(false);
	root->read();
}

void FS::mkfs()
{
	fs = this;
	open(true);

	if (!root->read()) {
		batch_t batch;
		root->write(batch);
		fs->write(batch, true);
	}
}

boost::shared_ptr<entry> FS::find(const std::string & path)
{
	return root->find(path);
}

boost::shared_ptr<entry> FS::find_parent(const std::string & path)
{
	boost::shared_ptr<entry> dst;
	size_t pos = path.rfind("/");
	if (pos == std::string::npos) {
		fprintf(l, "root parent %s\n", path.c_str());
		dst = root;
	} else {
		fprintf(l, "non root parent %s\n", path.c_str());
		dst = root->find(path.substr(0, pos));
	}
	return dst;
}

std::string FS::filename(const std::string & path)
{
	std::string name;
	size_t pos = path.rfind("/");
	if (pos == std::string::npos) {
		name = path;
	} else {
		name = path.substr(pos+1);
	}
	return name;
}

uint64_t FS::allocate_handle(const boost::shared_ptr<entry> & r, struct fuse_file_info *fi)
{
	boost::unique_lock<boost::mutex> scoped_lock(mutex);
	uint64_t fh = 0;
	for (; allocated_handles.find(fh) != allocated_handles.end(); ++fh);

	allocated_handles.insert(fh);
	fi->fh = fh;

	handles[fh] = r;

	return fh;
}

void FS::release_handle(uint64_t h)
{
	boost::unique_lock<boost::mutex> scoped_lock(mutex);
	allocated_handles.erase(h);
	boost::shared_ptr<entry> e = handles[h];
	handles[h].reset();
	if (e) {
		sync(e);
	}
}

boost::shared_ptr<entry> FS::find_handle(uint64_t t)
{
	return handles[t];
}

int FS::part(const block_key & key)
{
	if (key.type == 'd') {
		return 0;
	} else {
		return *((uint64_t*)key.inode)%parts+1;
	}
}

bool bucket::read(const block_key & key, std::string & value)
{
	boost::unique_lock<boost::mutex> scoped_lock(mutex);
//	fprintf(l, "read %p\n", this);
	std::map<block_key, operation>::iterator it = batch.find(key);
	if (it == batch.end()) {
		// read from ldb
//		fprintf(l, "not found in cache '%s', cache size %d\n", key.tostring().c_str(), (int)batch.size());
//		for (it = batch.begin(); it != batch.end(); ++it) {
//			fprintf(l, "  -> '%s'\n", it->first.tostring().c_str());
//		}
		leveldb::ReadOptions readOptions;
		leveldb::Status status;
		status = db->Get(readOptions, leveldb::Slice((char*)&key, key.size()), &value);
//		if (!status.ok()) {
//			fprintf(l, "not found on disk '%s'\n", key.tostring().c_str());
//		}
		return status.ok();
	} else if (it->second.type == operation::PUT) {
//		fprintf(l, "found in cache '%s'\n", key.tostring().c_str());
		value = it->second.data;
//		fprintf(l, "value -> '%s'\n", value.c_str());
		return true;
	} else {
//		fprintf(l, "delete found in cache '%s'\n", key.tostring().c_str());
		value.clear();
		return false;
	}
}

void bucket::add_op(const operation & op)
{
	boost::unique_lock<boost::mutex> scoped_lock(mutex);
//	fprintf(l, "add op to %p \n", this);
	batch.erase(op.key);
//	fprintf(l, "store in cache '%s' -> '%s'\n",
//	        op.key.tostring().c_str(), op.data.c_str());
	batch.insert(std::make_pair(op.key, op));
}

bool bucket::flush(uuid_t inode)
{
	leveldb::WriteBatch b;
	boost::unique_lock<boost::mutex> scoped_lock(mutex);

	if (batch.empty()) {
		return true;
	}

	fprintf(l, "flush %p\n", this);

	std::set<block_key> remove;

	for (std::map<block_key, operation>::iterator it = batch.begin();
	     it != batch.end(); ++it)
	{
		const block_key & key = it->first;
		operation & op = it->second;
		if (!inode || memcmp(key.inode, inode, sizeof(key.inode)) != 0) {
			continue;
		}
		leveldb::Slice slice((char*)&key, key.size());
		switch (op.type) {
		case operation::DELETE:
//			fprintf(l, "delete '%s' \n", key.tostring().c_str());
			b.Delete(slice);
			break;
		case operation::PUT:
			fprintf(l, "flush '%s' %lu bytes\n", key.tostring().c_str(),
			        op.data.size());
			b.Put(slice, op.data);
			break;
		}

		remove.insert(it->first);
	}

	leveldb::WriteOptions writeOptions;
	writeOptions.sync = true;
	
	sync = false;

	leveldb::Status status;
	status = db->Write(writeOptions, &b);

	for (std::set<block_key>::iterator it = remove.begin(); it != remove.end(); ++it)
	{
		batch.erase(*it);
	}

//	batch.clear();

//	if (!status.ok()) {
//		fprintf(l, "failed\n");
//	}

	return status.ok();
}

bool FS::read(const block_key & key, std::string & value)
{
	return buckets[part(key)].read(key, value);
}

bool FS::write(batch_t & batch, bool sync)
{
	leveldb::WriteOptions writeOptions;
	writeOptions.sync = sync;

	leveldb::WriteBatch dbatch;
	leveldb::WriteBatch fbatch;

	leveldb::DB * db = 0;
	leveldb::DB * fb = 0;

	for (int i = 0; i < batch.size(); ++i) {
		operation & op = batch[i];
		bucket & b = buckets[part(op.key)];
		b.add_op(op);
		b.sync = sync;
	}

	bool ret = true;
	for (int i = 0; i < parts+1; ++i) {
		if (buckets[i].sync) {
			ret &= buckets[i].flush(0);
		}
	}

	//TODO: sync by size
	return ret;
}

bool FS::sync(const boost::shared_ptr<entry> & e)
{
	block_key key(e->type, e->inode, 0);
	bucket & b = buckets[part(key)];
	return b.flush(e->inode);
}
