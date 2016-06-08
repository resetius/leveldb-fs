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
#include <boost/algorithm/string.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>

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


static FS * fs;

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


std::string dbpath;
boost::log::sources::severity_logger< >& lg = global_lg::get();

static void * ldbfs_init(struct fuse_conn_info *conn) {
//	conn->direct_io = 1;
	conn->max_write = 32*1024*1024;
	conn->want |= FUSE_CAP_BIG_WRITES;
	fs = new FS(dbpath);
	fs->mount();
}

static int ldbfs_getattr(const char *p, struct stat *stbuf)
{
	int res = 0;

	BOOST_LOG(lg) << "getattr " << p;
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

static int ldbfs_readdir(const char *, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;

	boost::shared_ptr<entry> d(fs->find_handle(fi->fh));
	
	if (!d) {
		return -ENOENT;
	}

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	boost::unique_lock<boost::mutex> scoped_lock(d->mutex);
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

	BOOST_LOG(lg) << "mkdir " << p;

	if (fs->find(path)) {
		BOOST_LOG(lg) << "already exists " << p;
		return -1; // already exists. TODO: check error code
	}

	boost::shared_ptr<entry> dst = fs->find_parent(path);
	name = fs->filename(path);

	if (!dst) {
		BOOST_LOG(lg) << "cannot find dst " << p;
		return -1; // parent not exists: TODO: check error code;
	}

	BOOST_LOG(lg) << "parent: " << dst->name << "/" << name;

	boost::shared_ptr<entry> r(new dentry(name, fs));
	dst->add_child(r);

	batch_t batch;

	r->write(batch);
	dst->write(batch);
	fs->write(batch, true); // TODO: check status

	return 0;
}

static int ldbfs_unlink(const char *p)
{
	BOOST_LOG(lg) << "unlink " << p;
	std::string path(p+1);

	boost::shared_ptr<entry> e = fs->find(path);
	if (!e) {
		return -ENOENT;
	}

	boost::shared_ptr<entry> dst = fs->find_parent(path);

	if (!dst) {
		BOOST_LOG(lg) << "cannot find dst " << p;
		return -1; // parent not exists: TODO: check error code;
	}

	// lock dst

	boost::mutex * m1 = &e->mutex;
	boost::mutex * m2 = &dst->mutex;

	assert(m1 != m2);

	if (m2 > m1) {
		std::swap(m1, m2);
	}

//	boost::unique_lock<boost::mutex> scoped_lock1(*m1);
//	boost::unique_lock<boost::mutex> scoped_lock2(*m2);

	batch_t batch;

	e->remove(batch);
	dst->remove_child(e);
	dst->write(batch);

	bool status = fs->write(batch, true); //TODO: check status


	if (!status) {
//		fprintf(l, "cannot remove %s %s\n",
//		        p, status.ToString().c_str());
		return -1;
	}

	return 0;
}

static int ldbfs_rmdir(const char *p)
{
	BOOST_LOG(lg) << "rmdir " << p;
	std::string path(p+1);

	boost::shared_ptr<entry> e = fs->find(path);
	if (!e) {
		return -ENOENT;
	}

//	boost::unique_lock<boost::mutex> scoped_lock(e->mutex);
	if (!e->entries.empty()) {
		return -1;
	}

	boost::shared_ptr<entry> parent = fs->find_parent(path);
	if (!parent) {
		return -1;
	}
	parent->remove_child(e);

	batch_t batch;

	parent->write(batch);
	e->remove(batch);
	bool status = fs->write(batch, true); //TODO: check status

	if (!status) {
//		fprintf(l, "cannot rmdir %s %s\n",
//		        p, status.ToString().c_str());
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

	batch_t batch;

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
		dst_parent->remove_child(dst);
	}

	// TODO: locks

	src_parent->remove_child(src);
	src->name = new_name;
	dst_parent->add_child(src);

	src_parent->write(batch);
	dst_parent->write(batch);
	bool status = fs->write(batch, true); //TODO: check status

	if (!status) {
//		fprintf(l, "cannot rename %s->%s %s\n",
//		        from.c_str(), to.c_str(),
//		        status.ToString().c_str());
		return -1;
	}


	return 0;
}

static int ldbfs_truncate(const char *p, off_t size)
{
	BOOST_LOG(lg) << "truncate " << p;
	std::string path(p+1);

	boost::shared_ptr<entry> e = fs->find(path);
	if (!e) {
		return -ENOENT;
	}

//	boost::unique_lock<boost::mutex> scoped_lock(e->mutex);

	batch_t batch;

	e->truncate(batch, size);
	bool status = fs->write(batch, true); //TODO: check status


	if (!status) {
//		fprintf(l, "cannot truncate %s %s\n",
//		        p, status.ToString().c_str());
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
		BOOST_LOG(lg) << "cannot find dst " << p;
		return -1; // parent not exists: TODO: check error code;
	}

	boost::shared_ptr<entry> r(new fentry(name, fs));
	dst->add_child(r);

	batch_t batch;

	r->write(batch);
	dst->write(batch);
	fs->write(batch, true); // TODO: check status

	fi->direct_io = 1;

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

static int ldbfs_release(const char *, struct fuse_file_info *fi)
{
	boost::shared_ptr<entry> r(fs->find_handle(fi->fh));
	if (!r) {
		return -1;
	}

//	fi->direct_io = 1;
	fs->release_handle(fi->fh);

	return 0;
}

static int ldbfs_read(
	const char *, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi)
{
	boost::shared_ptr<entry> d(fs->find_handle(fi->fh));
	if (!d) {
		return -1;
	}

//	boost::unique_lock<boost::mutex> scoped_lock(d->mutex);

	return d->read_buf(buf, size, offset);	
}

static int ldbfs_write(
	const char *, const char *buf, size_t size,
	off_t offset, struct fuse_file_info *fi)
{
	boost::shared_ptr<entry> d(fs->find_handle(fi->fh));
	if (!d) {
		return -1;
	}

//	boost::unique_lock<boost::mutex> scoped_lock(d->mutex);

	batch_t batch;

	int write_size = d->write_buf(batch, buf, size, offset);

	bool status = fs->write(batch, false); //TODO: check status
	if (!status) {
//		fprintf(l, "cannot write path %s %lu %lu %s\n",
//		        path, size, offset, status.ToString().c_str());
		return -1;
	}

	return write_size;
}

static int ldbfs_fsync(const char *, int isdatasync,
		     struct fuse_file_info *fi)
{
	(void) isdatasync;

	boost::shared_ptr<entry> d(fs->find_handle(fi->fh));
	if (!d) {
		return -1;
	}

	batch_t batch;

	if (!fs->sync(d)) {
		return -1;
	}

	return 0;
}

static int ldbfs_readlink(const char * link, char * target, size_t n)
{
	std::string path = link+1;
	boost::shared_ptr<entry> s(fs->find(path));
	if (!s) {
		// not exists
		return -1;
	}

	memset(target, 0, n);
	memcpy(target, s->target_name.c_str(), std::min(n, s->target_name.size()));
	return 0;
}

static int ldbfs_symlink(const char * src, const char * dst)
{
	// TODO: here may be links to external filesystem
	std::string src_path = src+1;
	std::string dst_path = dst+1;
	boost::shared_ptr<entry> s(fs->find(src_path));
	if (!s) {
		// not exists
		BOOST_LOG(lg) << "unknown source " << src;
		return -1;
	}
	boost::shared_ptr<entry> d(fs->find(dst_path));
	if (d) {
		// already exists
		BOOST_LOG(lg) << "already exists dest " << dst;
		return -1;
	}
	boost::shared_ptr<entry> parent(fs->find_parent(dst_path));
	if (!parent) {
		// not exists
		BOOST_LOG(lg) << "unknown parent " << dst;
		return -1;
	}
	d.reset(new symlink_entry(fs->filename(dst_path), fs));
	d->target_name = fs->filename(src_path);
	parent->add_child(d);

	batch_t batch;
	parent->write(batch);
	fs->write(batch, sync);

	return 0;
}

static int ldbfs_chown(const char * src, uid_t uid, gid_t gid)
{
	std::string path = src+1;
	boost::shared_ptr<entry> s(fs->find(path));
	if (!s) {
		// not exists
		BOOST_LOG(lg) << "unknown source " << src;
		return -1;
	}

	s->st.st_uid = uid;
	s->st.st_gid = gid;

	return 0;
}

static struct fuse_operations ldbfs_oper;

namespace keywords = boost::log::keywords;

int main(int argc, char *argv[])
{
	memset(&ldbfs_oper, 0, sizeof(ldbfs_oper));
	ldbfs_oper.getattr = ldbfs_getattr;
	ldbfs_oper.readdir = ldbfs_readdir;
	ldbfs_oper.open = ldbfs_open;
	ldbfs_oper.opendir = ldbfs_open;
	ldbfs_oper.read = ldbfs_read;
	ldbfs_oper.write = ldbfs_write;
	ldbfs_oper.access = ldbfs_access;
	ldbfs_oper.truncate = ldbfs_truncate;
	ldbfs_oper.create = ldbfs_create;
	ldbfs_oper.mkdir = ldbfs_mkdir;
	ldbfs_oper.release = ldbfs_release;
	ldbfs_oper.releasedir = ldbfs_release;
	ldbfs_oper.fsync = ldbfs_fsync;
	ldbfs_oper.unlink = ldbfs_unlink;
	ldbfs_oper.rmdir = ldbfs_rmdir;
	ldbfs_oper.rename = ldbfs_rename;
	ldbfs_oper.utime = ldbfs_utime;
//	ldbfs_oper.symlink = ldbfs_symlink;
//	ldbfs_oper.readlink = ldbfs_readlink;
//	ldbfs_oper.chown = ldbfs_chown;

	ldbfs_oper.init = ldbfs_init;

	ldbfs_oper.flag_nopath = 1;

	umask(0);

	char ** new_argv = (char**)calloc(argc+1, sizeof(char*));
	int j = 0;
	std::map<std::string, std::string> params;

	for (int i = 0; i < argc; ++i) {
		if (!strcmp(argv[i], "-p")) {
			std::string info = argv[++i];
			std::vector<std::string> infos;
			boost::algorithm::split(infos, info,
			                        boost::algorithm::is_any_of(","),
			                        boost::algorithm::token_compress_on);
			for (size_t i = 0; i < infos.size(); ++i) {
				size_t p = infos[i].find("=");
				if (p != std::string::npos) {
					std::string k = infos[i].substr(0, p);
					std::string v = infos[i].substr(p+1);
					params[k] = v;
				}
			}
		} else {
			new_argv[j++] = argv[i];
		}
	}

	std::map<std::string, std::string>::iterator it = params.find("db");
	if (it == params.end()) {
		return -1;
	}

	dbpath = it->second;

	it = params.find("log");
	if (it != params.end()) {
		boost::log::add_file_log(
			keywords::file_name = it->second,
			keywords::format = "[%TimeStamp%]: %Message%",
			keywords::auto_flush = true
		);
	} else {
		boost::log::add_console_log();
	}
	boost::log::core::get()->add_global_attribute(
		"TimeStamp", boost::log::attributes::local_clock());

	BOOST_LOG(lg) << "using root: " << dbpath;
	
	return fuse_main(j, new_argv, &ldbfs_oper, NULL);
}
