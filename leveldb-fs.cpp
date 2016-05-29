/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall fusexmp.c `pkg-config fuse --cflags --libs` -o fusexmp
*/

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
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include <string>
#include <algorithm>
#include <boost/unordered_map.hpp>

#include "dentry.h"

static const char *hello_path = "/hello";

int blocksize=4*1024;
leveldb::DB* db;

boost::shared_ptr<dentry> root;

FILE * l = fopen("fuselog.log", "a");;

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

static int xmp_getattr(const char *p, struct stat *stbuf)
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

static int xmp_access(const char *path, int mask)
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

static int xmp_unlink(const char *path)
{
	int res;

	res = unlink(path);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_rmdir(const char *path)
{
	int res;

	res = rmdir(path);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_rename(const char *from, const char *to)
{
	int res;

	res = rename(from, to);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_chmod(const char *path, mode_t mode)
{
	int res;

	res = chmod(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_chown(const char *path, uid_t uid, gid_t gid)
{
	int res;

	res = lchown(path, uid, gid);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_truncate(const char *path, off_t size)
{
	int res;

// TODO:
	return 0;
}

#ifdef HAVE_UTIMENSAT
static int xmp_utimens(const char *path, const struct timespec ts[2])
{
	int res;

	/* don't use utime/utimes since they follow symlinks */
	res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
	if (res == -1)
		return -errno;

	return 0;
}
#endif

static int xmp_open(const char *path, struct fuse_file_info *fi)
{
    if (strcmp(path, hello_path) != 0)
        return -ENOENT;
// TODO:check flags
//    if ((fi->flags & 3) != O_RDONLY)
//        return -EACCES;

	return 0;
}

static int xmp_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	/*
    size_t len;
    (void) fi;
    if(strcmp(path, hello_path) != 0)
        return -ENOENT;

    len = strlen(hello_str);
    if (offset < len) {
        if (offset + size > len)
            size = len - offset;
        memcpy(buf, hello_str + offset, size);
    } else
        size = 0;

	return size;
	*/

	fprintf(l, "read %s\n", path);

	int cur_block  = offset / blocksize;
	int cur_offset = offset;
	int read_size = 0;
	char * p = buf;
	leveldb::ReadOptions options;
	while (cur_offset < filesize) {
		char key[256];
		snprintf(key, 256, "%s04%d", hello_path, cur_block);
		std::string value;
		leveldb::Status st = db->Get(options, key, &value);
		if (!st.ok()) {
			fprintf(l, "cannot read key %s\n", key);
			break;
		}
		fprintf(l, "read key %s\n", key);
		int upto = std::min(buf + size - p, (long)value.size());
		read_size += upto;
		memcpy(p, value.c_str() + cur_offset % blocksize, value.size());
		p += upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;
	}

	return read_size;
}

static int xmp_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	// TODO: rewrite first block
	fprintf(l, "write %s %lu %lu\n", path, size, offset);
	int cur_block  = offset / blocksize;
	int cur_offset = offset;
	int write_size = 0;
	const char * p = buf;

	leveldb::WriteOptions writeOptions;
	leveldb::WriteBatch batch;
	fprintf(l, "cur offset %s %d\n", path, cur_offset);
	int upto = 1;

	int r = offset % blocksize;
	leveldb::ReadOptions options;

	if (r != 0) {
		char key[256];
		snprintf(key, 256, "%s04%d", hello_path, cur_block);
		std::string value;
		leveldb::Status st = db->Get(options, key, &value);

		value.resize(blocksize);

		char * dst = (char*)value.c_str() + r;
		upto = std::min((long)(blocksize - r), (long)size);
		memcpy(dst, p, upto);
		value.resize(r+upto);

		fprintf(l, "write(1)key %s\n", key);
		batch.Put(key, value);
		write_size += upto;

		p +=  upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;

		if (upto != blocksize - r) {
			upto = 0;
		}
	}

	while (upto > 0) {
		char key[256];
		snprintf(key, 256, "%s04%d", hello_path, cur_block);
		std::string value;
		upto = std::min(buf + size - p, (long)blocksize);
		if (upto == 0) {
			break;
		}
		fprintf(l, "write key %s\n", key);
		batch.Put(key, leveldb::Slice(p, upto));
		write_size += upto;
		p += upto;
		cur_block ++;
		cur_offset = cur_block * blocksize;
	}

	db->Write(writeOptions, &batch);

	filesize = std::max((long)filesize, (long)(offset+size));

	fprintf(l, "written %s %d\n", path, write_size);

	return write_size;
}

static int xmp_release(const char *path, struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */

	(void) path;
	(void) fi;
	return 0;
}

static int xmp_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */

	(void) path;
	(void) isdatasync;
	(void) fi;
	return 0;
}

static struct fuse_operations xmp_oper;
#if 0
= {
	.getattr	= xmp_getattr,
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
	xmp_oper.getattr = xmp_getattr;
	xmp_oper.readdir = ldbfs_readdir;
	xmp_oper.open = xmp_open;
	xmp_oper.read = xmp_read;
	xmp_oper.write = xmp_write;
	xmp_oper.access = xmp_access;
	xmp_oper.truncate = xmp_truncate;
	xmp_oper.mkdir = ldbfs_mkdir;

	umask(0);

	leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kNoCompression;

    
    leveldb::WriteOptions wo;
    wo.sync = true;
    
//    options.write_buffer_size = 4*1024*1024;
	leveldb::Status status = leveldb::DB::Open(options, "./testdb-fs", &db);

	root.reset(new dentry(""));
	if (!root->read()) {
		leveldb::WriteBatch batch;
		root->write(batch);
		db->Write(wo, &batch);
	}
	
	setbuf(l, 0);

	return fuse_main(argc, argv, &xmp_oper, NULL);
}
