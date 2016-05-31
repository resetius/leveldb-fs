#include "fs.h"

FILE * l = 0;
FS * fs = 0;

FS::FS()
{
	maxhandles=1000000;
	blocksize=4*1024;

	l = fopen("/var/tmp/fuselog.log", "w");
	setbuf(l, 0);


	fprintf(l, "init\n");

	leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kNoCompression;

    handles.resize(maxhandles);
    
	leveldb::Status status = leveldb::DB::Open(options, "/var/tmp/testdb-fs", &db);

	root.reset(new dentry(""));
}

void FS::mount()
{
	root->read();
}

void FS::mkfs()
{
    leveldb::WriteOptions wo;
    wo.sync = true;  
//    options.write_buffer_size = 4*1024*1024;

	if (!root->read()) {
		leveldb::WriteBatch batch;
		root->write(batch);
		db->Write(wo, &batch);
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
	// TODO: lock
	uint64_t fh = 0;
	for (; allocated_handles.find(fh) != allocated_handles.end(); ++fh);

	allocated_handles.insert(fh);
	fi->fh = fh;

	handles[fh] = r;

	return fh;
}

void FS::release_handle(uint64_t h)
{
	allocated_handles.erase(h);
	handles[h].reset();
}

boost::shared_ptr<entry> FS::find_handle(uint64_t t)
{
	return handles[t];
}

leveldb::Status FS::write(leveldb::WriteBatch & batch, bool sync)
{
	leveldb::WriteOptions writeOptions;
	writeOptions.sync = sync;
	return db->Write(writeOptions, &batch);
}
