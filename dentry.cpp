#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "messages.pb.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "dentry.h"
#include "fs.h"

entry::entry(const std::string & name, FS * fs): fs(fs), l(fs->l), name(name)
{
	time_t now = time(0);
	memset(&st, 0, sizeof(st));
	st.st_atime = now;
	st.st_ctime = now;
	st.st_mtime = now;
	st.st_blksize = 4096; // fixme
	uuid_generate(inode);
}

std::string entry::stringify(const std::string & key)
{
	std::string r;
	char buf[256];
	for (std::string::const_iterator it = key.begin(); it != key.end(); ++it)
	{
		snprintf(buf, sizeof(buf), "%02hhx", *it);
		r += buf;
	}
	return r;
}

dentry::dentry(const std::string & name, FS * fs): entry(name, fs)
{
	st.st_mode = S_IFDIR | 0755;
	st.st_nlink = 2;
	st.st_size = 4096;
	if (name.empty()) {
		memset(inode, 0, sizeof(inode));
	}
	type = 'd';
}

fentry::fentry(const std::string & name, FS * fs): entry(name, fs)
{
	st.st_mode = S_IFREG | 0666;
	type = 'f';
}

symlink_entry::symlink_entry(const std::string & name, FS * fs): dentry(name, fs)
{
	st.st_mode = S_IFLNK | 0666;
	type = 's';
}

bool entry::read()
{
	std::string value;

	block_key key(type, inode);

	if (!fs->read(key, value)) {
//		fprintf(l, "key '%s' not found '%s'\n",
//		        key.tostring().c_str(),
//		        name.c_str());
		return false;
	}

	proto::entry e;

	fprintf(l, "read dentry '%s' -> %lu\n", name.c_str(), value.size());

	if (!e.ParseFromString(value)) {
		// TODO: error;
		fprintf(l, "cannot parse proto '%s'\n", name.c_str());
		return false;
	}

	if (e.has_ctime()) {
		st.st_ctime = e.ctime();
	}
	if (e.has_atime()) {
		st.st_atime = e.atime();
	}
	if (e.has_mtime()) {
		st.st_mtime = e.mtime();
	}
	if (e.has_size()) {
		st.st_size = e.size();
	}

	for (int i = 0; i < e.children_size(); ++i) {
		const proto::entry_child & c = e.children(i);
		std::string name = c.name();
		entry * d = 0;
		if (c.mode() & S_IFDIR) {
			fprintf(l, "addin dir to '%s'\n", name.c_str());
			d = new dentry(name, fs);
		} else if (c.mode() & S_IFREG) {
			fprintf(l, "addin file to '%s'\n", name.c_str());
			d = new fentry(name, fs);
		} else if (c.mode() & S_IFLNK) {
			fprintf(l, "addin symlink to '%s'\n", name.c_str());
			d = new symlink_entry(name, fs);
			d->target_name = c.target_name();
		}

		if (d) {
			std::string inode = c.ino();
			memcpy(d->inode, inode.c_str(), sizeof(d->inode)); //TODO: ugly
			if (d->read()) {
				entries[name] = boost::shared_ptr<entry>(d);
				//fprintf(l, "adding object to '%s' -> %s %lu\n",
				//        this->name.c_str(),
				//        stringify(d->key()).c_str(),
				//        d->st.st_size);
			} else {
				// TODO: error
				// TODO: make broken entry
				fprintf(l, "cannot read '%s'\n", name.c_str());
				return false;
			}
		}
	}

	return true;
}

void entry::write(batch_t & batch)
{
	std::string value;

	time_t now = time(0);

	st.st_mtime = now;
	st.st_atime = now;
	
	proto::entry e;
	e.set_mode(st.st_mode);
	e.set_mtime(st.st_mtime);
	e.set_ctime(st.st_ctime);
	e.set_size(st.st_size);

	for (entries_t::iterator it = entries.begin(); it != entries.end(); ++it) {
		proto::entry_child * c = e.add_children();
		c->set_mode(it->second->st.st_mode);
		c->set_ino(it->second->inode, sizeof(it->second->inode));
		c->set_name(it->second->name);
		if (it->second->type == 's') {
			c->set_target_name(it->second->target_name);
		}

//		fprintf(l, "adding to entry '%s' -> %u,  %s \n",
//		        name.c_str(), it->second->st.st_mode,
//		        stringify(it->second->key()).c_str());
	}

	e.SerializeToString(&value); // TODO: check error

	block_key key(type, inode);

//	fprintf(l, "writing entry '%s' -> '%s' \n",
//	        name.c_str(), key.tostring().c_str());
	
	batch.push_back(operation(key, operation::PUT, value));
}

boost::shared_ptr<entry> entry::find(const std::string & path)
{
	fprintf(l, "find '%s' in '%s'\n", path.c_str(), name.c_str());
	if (name == path) {
		return shared_from_this();
	}

	size_t pos = path.find("/");
	std::string subname = path.substr(0, pos);

	boost::shared_ptr<entry> e;
	{
		boost::unique_lock<boost::mutex> scoped_lock(mutex);
		entries_t::iterator it = entries.find(subname);
		if (it == entries.end()) {
			return boost::shared_ptr<entry>();
		}
		e = it->second;		
	}
	return e->find(path.substr(pos+1));
}

void entry::add_child(const boost::shared_ptr<entry> & e)
{
	boost::unique_lock<boost::mutex> scoped_lock(mutex);
	entries[e->name] = e;
}

void entry::remove_child(const std::string & name)
{
	boost::unique_lock<boost::mutex> scoped_lock(mutex);
	entries.erase(name);
}

void entry::remove_child(const boost::shared_ptr<entry> & e)
{
	boost::unique_lock<boost::mutex> scoped_lock(mutex);
	entries.erase(e->name);
}

void entry::fillstat(struct stat * s)
{
	memcpy(s, &st, sizeof(st));
	s->st_blocks = (s->st_size + 511) / 512;
	memcpy(&s->st_ino, inode, sizeof(s->st_ino)); 
}

void dentry::remove(batch_t & batch)
{
	block_key key(type, inode);
	batch.push_back(operation(key, operation::DELETE, std::string()));
}
