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
    status = leveldb::DB::Open(options, dbroot + "/dentry", &meta);
    data.resize(parts);
    for (int i = 0; i < parts; ++i) {
	    char buf[1024];
	    snprintf(buf, sizeof(buf), "/fentry-%04d", i);
	    status = leveldb::DB::Open(options, dbroot + buf, &data[i]);
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
	handles[h].reset();
}

boost::shared_ptr<entry> FS::find_handle(uint64_t t)
{
	return handles[t];
}

bool FS::read(const block_key & key, std::string & value)
{
	leveldb::ReadOptions readOptions;
	leveldb::Status status;

	leveldb::DB * db = 0;
	
	if (key.type == 'd') {
		db = meta;
	} else {
		uint64_t part;
		memcpy(&part, key.inode, sizeof(part));
		part = part % parts;
		leveldb::DB * fbnew = data[part];
		assert(db == 0 || fbnew == fb);
		db = fbnew;
	}

	status = db->Get(readOptions, leveldb::Slice((char*)&key, key.size()), &value);
//	fprintf(l, "get key '%s'\n", key.tostring().c_str());
	return status.ok();
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
		leveldb::WriteBatch * b;
		if (batch[i].key.type == 'd') {
			db = meta;
			b = &dbatch;
		} else {
			uint64_t part;
			memcpy(&part, batch[i].key.inode, sizeof(part));
			part = part % parts;
			leveldb::DB * fbnew = data[part];
			assert(db == 0 || fbnew == fb);
			fb = fbnew;
			b = &fbatch;
		}

		leveldb::Slice slice((char*)&batch[i].key, batch[i].key.size());
		switch (batch[i].type) {
		case operation::DELETE:
//			fprintf(l, "delete key '%s'\n", batch[i].key.tostring().c_str());
			b->Delete(slice);
			break;
		case operation::PUT:
//			fprintf(l, "put key '%s'\n", batch[i].key.tostring().c_str());
			b->Put(slice, batch[i].data);
			break;
		}
	}

	leveldb::Status status;
	if (db) {
//		fprintf(l, "flushin metadata (%d)\n", (int)sync);
		status = db->Write(writeOptions, &dbatch);
	}
	if (fb) {
//		fprintf(l, "flushin data (%d)\n", (int)sync);
		status = fb->Write(writeOptions, &fbatch);
	}
	
	return status.ok();
}
