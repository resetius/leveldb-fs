
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
#include "fs.h"

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


extern FILE * l;
extern FS * fs;

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
	fs = new FS();
	fs->mount();
}

static int ldbfs_getattr(const char *p, struct stat *stbuf)
{
	int res = 0;

	fprintf(l, "getattr %s\n", p);
    memset(stbuf, 0, sizeof(struct stat));

	boost::shared_ptr<entry> e = fs->find(p+1);
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

	boost::shared_ptr<entry> d(fs->find(path+1));
	
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

	if (fs->find(path)) {
		fprintf(l, "already exists %s\n", p);
		return -1; // already exists. TODO: check error code
	}

	boost::shared_ptr<entry> dst = fs->find_parent(path);
	name = fs->filename(path);

	if (!dst) {
		fprintf(l, "cannot find dst %s\n", p);
		return -1; // parent not exists: TODO: check error code;
	}

	fprintf(l, "parent: '%s'/'%s'\n", dst->name.c_str(), name.c_str());

	boost::shared_ptr<entry> r(new dentry(name));
	dst->entries[name] = r;

	leveldb::WriteBatch batch;

	r->write(batch);
	dst->write(batch);
	fs->write(batch, true); // TODO: check status

	return 0;
}

static int ldbfs_unlink(const char *p)
{
	fprintf(l, "unlink %s\n", p);
	std::string path(p+1);

	boost::shared_ptr<entry> e = fs->find(path);
	if (!e) {
		return -ENOENT;
	}

	boost::shared_ptr<entry> dst = fs->find_parent(path);

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

	leveldb::WriteBatch batch;

	e->remove(batch);
	dst->entries.erase(e->name);
	dst->write(batch);

	leveldb::Status status = fs->write(batch, true); //TODO: check status


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

	boost::shared_ptr<entry> e = fs->find(path);
	if (!e) {
		return -ENOENT;
	}

	boost::unique_lock<boost::mutex> scoped_lock(e->mutex);
	if (!e->entries.empty()) {
		return -1;
	}

	leveldb::WriteBatch batch;

	e->remove(batch);
	leveldb::Status status = fs->write(batch, true); //TODO: check status

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

	boost::shared_ptr<entry> src = fs->find(from);
	if (!src) {
		return -ENOENT;
	}

	leveldb::WriteBatch batch;

	boost::shared_ptr<entry> dst = fs->find(to);

	boost::shared_ptr<entry> src_parent = fs->find_parent(from);
	boost::shared_ptr<entry> dst_parent = fs->find_parent(to);	
	std::string new_name = fs->filename(to);

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
	leveldb::Status status = fs->write(batch, true); //TODO: check status

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

	boost::shared_ptr<entry> e = fs->find(path);
	if (!e) {
		return -ENOENT;
	}

	boost::unique_lock<boost::mutex> scoped_lock(e->mutex);

	leveldb::WriteBatch batch;

	e->truncate(batch, size);
	leveldb::Status status = fs->write(batch, true); //TODO: check status


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

static int ldbfs_create(const char *p, mode_t mode,
                        struct fuse_file_info *fi)
{
	std::string path = p+1;
	boost::shared_ptr<entry> d(fs->find(path));
	if (d) {
		// already exists
		return -1;
	}
		
	boost::shared_ptr<entry> dst = fs->find_parent(path);
	std::string name = fs->filename(path);

	if (!dst) {
		fprintf(l, "cannot find dst %s\n", p);
		return -1; // parent not exists: TODO: check error code;
	}

	boost::shared_ptr<entry> r(new fentry(name));
	dst->entries[name] = r;

	leveldb::WriteBatch batch;

	r->write(batch);
	dst->write(batch);
	fs->write(batch, true); // TODO: check status

	fs->allocate_handle(r, fi);

	return 0;
}

static int ldbfs_open(const char *p, struct fuse_file_info *fi)
{
	boost::shared_ptr<entry> d(fs->find(p+1));
	if (!d) {
		return -1;
	}

	fs->allocate_handle(d, fi);

	return 0;
}

static int ldbfs_release(const char *path, struct fuse_file_info *fi)
{
	fprintf(l, "release '%s' -> %lu\n", path, fi->fh);
	boost::shared_ptr<entry> r(fs->find_handle(fi->fh));
	if (!r) {
		return -1;
	}

	fs->release_handle(fi->fh);

	return 0;
}

static int ldbfs_read(
	const char *path, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi)
{
	fprintf(l, "read %s\n", path);

	boost::shared_ptr<entry> d(fs->find_handle(fi->fh));
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

	boost::shared_ptr<entry> d(fs->find_handle(fi->fh));
	if (!d) {
		return -1;
	}

	boost::unique_lock<boost::mutex> scoped_lock(d->mutex);

	leveldb::WriteBatch batch;

	int write_size = d->write_buf(batch, buf, size, offset);

	leveldb::Status status = fs->write(batch, false); //TODO: check status
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

	boost::shared_ptr<entry> d(fs->find_handle(fi->fh));
	if (!d) {
		return -1;
	}

	leveldb::WriteBatch batch;

	d->write(batch);
	leveldb::Status status = fs->write(batch, true); //TODO: check status

	if (!status.ok()) {
		fprintf(l, "cannot sync %s %s\n",
		        path, status.ToString().c_str());
		return -1;
	}

	return 0;
}

static struct fuse_operations ldbfs_oper;

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
