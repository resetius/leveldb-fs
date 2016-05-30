
#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include <string>
#include <algorithm>
#include <vector>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "dentry.h"

static const char *hello_path = "/hello";

int maxhandles=1000000;
int blocksize=4*1024;
leveldb::DB* db;

boost::shared_ptr<dentry> root;
// TODO: remove while writing?
// remove algo proposal:
// 1. mark removed inode in superblock
// 2. in destructor batch:
//   2.1 remove blocks
//   2.2 remove inode
//   2.3 unmark inode
// 3. on powerfailure repeat from 2
//
// truncate algo proposal:
// 1. mark new size in superblock
// 2. batch
//   2.1 remove/update blocks
//   2.2 update inode
//   2.3 unmark
// 3. on powerfailure repeat from 2

std::vector<boost::shared_ptr<entry> > handles;
boost::unordered_set<uint64_t> allocated_handles;

FILE * l = 0;

int filesize=0;

// dentry -> [type,entry]
// (f,entry) -> (inode,name)
// (d,entry) -> name
// inode -> filesize

// create file:
// append new (inode,name) into direntry
// create dir:
// new direntry

// direntry: d,path
// inode: i,number
// block: b,inumber,bnumber

static void * ldbfs_init(struct fuse_conn_info *conn) {
	l = fopen("/var/tmp/fuselog.log", "w");
	setbuf(l, 0);


	fprintf(l, "init\n");

	leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kNoCompression;

    handles.resize(maxhandles);
    
    leveldb::WriteOptions wo;
    wo.sync = true;  
//    options.write_buffer_size = 4*1024*1024;
	leveldb::Status status = leveldb::DB::Open(options, "/var/tmp/testdb-fs", &db);

	root.reset(new dentry(""));
	if (!root->read()) {
		leveldb::WriteBatch batch;
		root->write(batch);
		db->Write(wo, &batch);
	}

}

static int ldbfs_getattr(const char *p, struct stat *stbuf)
{
	int res = 0;

	fprintf(l, "getattr %s\n", p);
    memset(stbuf, 0, sizeof(struct stat));

	boost::shared_ptr<entry> e = root->find(p+1);
	if (!e) {
		res = -ENOENT;
	} else {
		e->fillstat(stbuf);
	}
		
	return res;
}

static int ldbfs_access(const char *path, int mask)
{
	return 0;
}

static int ldbfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;

	fprintf(l, "readdir %s\n", path);

	boost::shared_ptr<entry> d(root->find(path+1));
	
	if (!d) {
		return -ENOENT;
	}

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	for (entries_t::iterator it = d->entries.begin();
	     it != d->entries.end(); ++it)
	{
		filler(buf, it->second->name.c_str(), NULL, 0);
	}

	return 0;
}

static int ldbfs_mkdir(const char *p, mode_t mode)
{
	std::string path(p+1);
	std::string name;

	fprintf(l, "mkdir %s\n", p);

	if (root->find(path)) {
		fprintf(l, "already exists %s\n", p);
		return -1; // already exists. TODO: check error code
	}

	boost::shared_ptr<entry> dst;
	size_t pos = path.rfind("/");
	if (pos == std::string::npos) {
		fprintf(l, "root parent %s\n", p);
		dst = root;
		name = path;
	} else {
		fprintf(l, "non root parent %s\n", p);
		dst = root->find(path.substr(0, pos));
		name = path.substr(pos+1);
	}

	if (!dst) {
		fprintf(l, "cannot find dst %s\n", p);
		return -1; // parent not exists: TODO: check error code;
	}

	fprintf(l, "parent: '%s'/'%s'\n", dst->name.c_str(), name.c_str());

	boost::shared_ptr<entry> r(new dentry(name));
	dst->entries[name] = r;

	leveldb::WriteOptions writeOptions;
	leveldb::WriteBatch batch;

	writeOptions.sync = true;
	r->write(batch);
	dst->write(batch);
	db->Write(writeOptions, &batch); // TODO: check status

	return 0;
}

static int ldbfs_unlink(const char *p)
{
	fprintf(l, "unlink %s\n", p);
	std::string path(p+1);

	boost::shared_ptr<entry> e = root->find(p+1);
	if (!e) {
		return -ENOENT;
	}

	boost::shared_ptr<entry> dst;
	size_t pos = path.rfind("/");
	if (pos == std::string::npos) {
		fprintf(l, "root parent %s\n", p);
		dst = root;
	} else {
		fprintf(l, "non root parent %s\n", p);
		dst = root->find(path.substr(0, pos));
	}

	if (!dst) {
		fprintf(l, "cannot find dst %s\n", p);
		return -1; // parent not exists: TODO: check error code;
	}

	// lock dst

	boost::mutex * m1 = &e->mutex;
	boost::mutex * m2 = &dst->mutex;

	assert(m1 != m2);

	if (m2 > m1) {
		std::swap(m1, m2);
	}

	boost::unique_lock<boost::mutex> scoped_lock1(*m1);
	boost::unique_lock<boost::mutex> scoped_lock2(*m2);

	leveldb::WriteOptions options;
	leveldb::WriteBatch batch;
	options.sync = true;

	e->remove(batch);
	dst->entries.erase(e->name);
	dst->write(batch);

	leveldb::Status status = db->Write(options, &batch); //TODO: check status


	if (!status.ok()) {
		fprintf(l, "cannot remove %s %s\n",
		        p, status.ToString().c_str());
		return -1;
	}

	return 0;
}

static int ldbfs_rmdir(const char *p)
{
	fprintf(l, "rmdir %s\n", p);
	std::string path(p+1);

	boost::shared_ptr<entry> e = root->find(p+1);
	if (!e) {
		return -ENOENT;
	}

	boost::unique_lock<boost::mutex> scoped_lock(e->mutex);
	if (!e->entries.empty()) {
		return -1;
	}

	leveldb::WriteOptions options;
	leveldb::WriteBatch batch;
	options.sync = true;

	e->remove(batch);
	leveldb::Status status = db->Write(options, &batch); //TODO: check status


	if (!status.ok()) {
		fprintf(l, "cannot rmdir %s %s\n",
		        p, status.ToString().c_str());
		return -1;
	}

	return 0;
}

static int ldbfs_rename(const char *f, const char *t)
{
	std::string from(f+1);
	std::string to(t+1);

	boost::shared_ptr<entry> src = root->find(from);
	if (!src) {
		return -ENOENT;
	}

	leveldb::WriteOptions options;
	leveldb::WriteBatch batch;
	options.sync = true;

	boost::shared_ptr<entry> dst = root->find(to);

	boost::shared_ptr<entry> src_parent;
	size_t pos = from.rfind("/");
	if (pos == std::string::npos) {
		fprintf(l, "root parent %s\n", f);
		src_parent = root;
	} else {
		fprintf(l, "non root parent %s\n", f);
		src_parent = root->find(from.substr(0, pos));
	}
	boost::shared_ptr<entry> dst_parent;
	pos = to.rfind("/");
	std::string new_name;
	if (pos == std::string::npos) {
		fprintf(l, "root parent %s\n", t);
		dst_parent = root;
		new_name = to;
	} else {
		fprintf(l, "non root parent %s\n", t);
		dst_parent = root->find(to.substr(0, pos));
		new_name = to.substr(pos+1, 0);
	}

	if (!src_parent || !dst_parent) {
		return -1;
	}

	if (dst) {
		if (dst == src) {
			return -1;
		}
		dst->remove(batch);
		dst_parent->entries.erase(dst->name);
	}

	// TODO: locks

	src_parent->entries.erase(src->name);
	src->name = new_name;
	dst_parent->entries[new_name] = src;

	src_parent->write(batch);
	dst_parent->write(batch);
	leveldb::Status status = db->Write(options, &batch); //TODO: check status

	if (!status.ok()) {
		fprintf(l, "cannot rename %s->%s %s\n",
		        from.c_str(), to.c_str(),
		        status.ToString().c_str());
		return -1;
	}


	return 0;
}

static int ldbfs_truncate(const char *p, off_t size)
{
	fprintf(l, "truncate %s\n", p);
	std::string path(p+1);

	boost::shared_ptr<entry> e = root->find(p+1);
	if (!e) {
		return -ENOENT;
	}

	boost::unique_lock<boost::mutex> scoped_lock(e->mutex);

	leveldb::WriteOptions options;
	leveldb::WriteBatch batch;
	options.sync = true;

	e->truncate(batch, size);
	leveldb::Status status = db->Write(options, &batch); //TODO: check status


	if (!status.ok()) {
		fprintf(l, "cannot truncate %s %s\n",
		        p, status.ToString().c_str());
		return -1;
	}

	return 0;
}

static int ldbfs_utime(const char *path, struct utimbuf * t)
{
	return 0;
}

uint64_t allocate_handle(struct fuse_file_info *fi)
{
	// TODO: lock
	uint64_t fh = 0;
	for (; allocated_handles.find(fh) != allocated_handles.end(); ++fh);

	allocated_handles.insert(fh);
	fi->fh = fh;
	return fh;
}


static int ldbfs_create(const char *p, mode_t mode,
                        struct fuse_file_info *fi)
{
	std::string path = p+1;
	boost::shared_ptr<entry> d(root->find(path));
	if (d) {
		// already exists
		return -1;
	}

	uint64_t fh = allocate_handle(fi);
	
	fprintf(l, "create '%s' -> %lu\n", p, fh);

	boost::shared_ptr<entry> dst;
	size_t pos = path.rfind("/");
	std::string name;

	if (pos == std::string::npos) {
		fprintf(l, "root parent %s\n", p);
		dst = root;
		name = path;
	} else {
		fprintf(l, "non root parent %s\n", p);
		dst = root->find(path.substr(0, pos));
		name = path.substr(pos+1);
	}

	if (!dst) {
		fprintf(l, "cannot find dst %s\n", p);
		return -1; // parent not exists: TODO: check error code;
	}

	boost::shared_ptr<entry> r(new fentry(name));
	dst->entries[name] = r;

	leveldb::WriteOptions writeOptions;
	leveldb::WriteBatch batch;

	writeOptions.sync = true;
	r->write(batch);
	dst->write(batch);
	db->Write(writeOptions, &batch); // TODO: check status

	handles[fh] = r;

	return 0;
}

static int ldbfs_open(const char *p, struct fuse_file_info *fi)
{
	boost::shared_ptr<entry> d(root->find(p+1));
	if (!d) {
		return -1;
	}

	uint64_t fh = allocate_handle(fi);

	fprintf(l, "open '%s' -> %lu\n", p, fh);

	handles[fh] = d;

	return 0;
}

static int ldbfs_release(const char *path, struct fuse_file_info *fi)
{
	fprintf(l, "release '%s' -> %lu\n", path, fi->fh);
	boost::shared_ptr<entry> r(handles[fi->fh]);
	if (!r) {
		return -1;
	}

	allocated_handles.erase(fi->fh);
	handles[fi->fh].reset();

	return 0;
}

static int ldbfs_read(
	const char *path, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi)
{
	fprintf(l, "read %s\n", path);

	boost::shared_ptr<entry> d(handles[fi->fh]);
	if (!d) {
		return -1;
	}

	boost::unique_lock<boost::mutex> scoped_lock(d->mutex);

	return d->read_buf(buf, size, offset);	
}

static int ldbfs_write(
	const char *path, const char *buf, size_t size,
	off_t offset, struct fuse_file_info *fi)
{
//	fprintf(l, "write %s %lu %lu\n", path, size, offset);

	boost::shared_ptr<entry> d(handles[fi->fh]);
	if (!d) {
		return -1;
	}

	boost::unique_lock<boost::mutex> scoped_lock(d->mutex);

	leveldb::WriteOptions options;
	leveldb::WriteBatch batch;
//	options.sync = true;

	int write_size = d->write_buf(batch, buf, size, offset);

	leveldb::Status status = db->Write(options, &batch); //TODO: check status
	if (!status.ok()) {
		fprintf(l, "cannot write path %s %lu %lu %s\n",
		        path, size, offset, status.ToString().c_str());
		return -1;
	}

	return write_size;
}

static int ldbfs_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	(void) path;
	(void) isdatasync;

	boost::shared_ptr<entry> d(handles[fi->fh]);
	if (!d) {
		return -1;
	}

	leveldb::WriteOptions options;
	leveldb::WriteBatch batch;
	options.sync = true;

	d->write(batch);
	leveldb::Status status = db->Write(options, &batch); //TODO: check status

	if (!status.ok()) {
		fprintf(l, "cannot sync %s %s\n",
		        path, status.ToString().c_str());
		return -1;
	}

	return 0;
}

static struct fuse_operations ldbfs_oper;
#if 0
= {
	.getattr	= ldbfs_getattr,
//	.access		= xmp_access,
	.readdir	= ldbfs_readdir,
//	.mkdir		= xmp_mkdir,
//	.unlink		= xmp_unlink,
//	.rmdir		= xmp_rmdir,
//	.rename		= xmp_rename,
//	.chmod		= xmp_chmod,
//	.chown		= xmp_chown,
//	.truncate	= xmp_truncate,
#ifdef HAVE_UTIMENSAT
//	.utimens	= xmp_utimens,
#endif
	.open		= xmp_open,
	.read		= xmp_read,
//	.write		= xmp_write,
//	.release	= xmp_release,
//	.fsync		= xmp_fsync
};
#endif

int main(int argc, char *argv[])
{
	ldbfs_oper.getattr = ldbfs_getattr;
	ldbfs_oper.readdir = ldbfs_readdir;
	ldbfs_oper.open = ldbfs_open;
	ldbfs_oper.read = ldbfs_read;
	ldbfs_oper.write = ldbfs_write;
	ldbfs_oper.access = ldbfs_access;
	ldbfs_oper.truncate = ldbfs_truncate;
	ldbfs_oper.create = ldbfs_create;
	ldbfs_oper.mkdir = ldbfs_mkdir;
	ldbfs_oper.release = ldbfs_release;
	ldbfs_oper.fsync = ldbfs_fsync;
	ldbfs_oper.unlink = ldbfs_unlink;
	ldbfs_oper.rmdir = ldbfs_rmdir;
	ldbfs_oper.rename = ldbfs_rename;
	ldbfs_oper.utime = ldbfs_utime;

	ldbfs_oper.init = ldbfs_init;

	umask(0);

	return fuse_main(argc, argv, &ldbfs_oper, NULL);
}
