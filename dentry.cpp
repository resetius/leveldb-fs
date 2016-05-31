#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "messages.pb.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "dentry.h"
#include "fs.h"

extern FILE * l;
extern FS * fs;

entry::entry(const std::string & name): name(name)
{
	time_t now = time(0);
	memset(&st, 0, sizeof(st));
	st.st_atime = now;
	st.st_ctime = now;
	st.st_mtime = now;
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

dentry::dentry(const std::string & name): entry(name)
{
	st.st_mode = S_IFDIR | 0755;
	if (name.empty()) {
		memset(inode, 0, sizeof(inode));
	}
}

fentry::fentry(const std::string & name): entry(name)
{
	st.st_mode = S_IFREG | 0666;
}

bool entry::read()
{
	std::string value;

	block_key key;
	key.type = type();
	memcpy(key.inode, inode, sizeof(inode));
	key.blockno = -1;	

	if (!fs->read(key, value)) {
		fprintf(l, "key not found '%s'\n", name.c_str());
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
			d = new dentry(name);
		} else if (c.mode() & S_IFREG) {
			fprintf(l, "addin file to '%s'\n", name.c_str());
			d = new fentry(name);
		}

		if (d) {
			std::string inode = c.ino();
			memcpy(d->inode, inode.c_str(), sizeof(d->inode)); //TODO: ugly
			if (d->read()) {
				entries[name] = boost::shared_ptr<entry>(d);
//				fprintf(l, "adding object to '%s' -> %s %lu\n",
//				        this->name.c_str(),
//				        stringify(d->key()).c_str(),
//				        d->st.st_size);
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

//	fprintf(l, "writing entry '%s' \n", name.c_str());

	for (entries_t::iterator it = entries.begin(); it != entries.end(); ++it) {
		proto::entry_child * c = e.add_children();
		c->set_mode(it->second->st.st_mode);
		c->set_ino(it->second->inode, sizeof(it->second->inode));
		c->set_name(it->second->name);

//		fprintf(l, "adding to entry '%s' -> %u,  %s \n",
//		        name.c_str(), it->second->st.st_mode,
//		        stringify(it->second->key()).c_str());
	}

	e.SerializeToString(&value); // TODO: check error

	block_key key;
	key.type = type();
	memcpy(key.inode, inode, sizeof(inode));
	key.blockno = -1;
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
	entries_t::iterator it = entries.find(subname);
	if (it == entries.end()) {
		return boost::shared_ptr<entry>();
	} else {
		return it->second->find(path.substr(pos+1));
	}
}

void fentry::fillstat(struct stat * s)
{
	memcpy(s, &st, sizeof(st));
	memcpy(&s->st_ino, inode, sizeof(s->st_ino)); 
}

void dentry::fillstat(struct stat * s)
{
	memcpy(s, &st, sizeof(st));
	memcpy(&s->st_ino, inode, sizeof(s->st_ino)); 
}

void dentry::remove(batch_t & batch)
{
	block_key key;
	key.type = type();
	memcpy(key.inode, inode, sizeof(inode));
	key.blockno = -1;
	batch.push_back(operation(key, operation::DELETE, std::string()));
}
