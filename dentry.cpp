#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "dentry.h"

extern leveldb::DB* db;
extern FILE * l;

dentry::dentry(const std::string & name): entry(name)
{
	if (name.empty()) {
		memset(inode, 0, sizeof(inode));
	} else {
		uuid_generate(inode);
		fprintf(l, "new dentry %s\n", name.c_str());
	}
}

bool dentry::read()
{
	std::string value;
	leveldb::ReadOptions options;
	leveldb::Status st = db->Get(options, key(), &value);
	if (!st.ok()) {
		return false;
	}

	fprintf(l, "read dentry '%s' -> %lu\n", name.c_str(), value.size());

	const char * p = value.c_str();
	const char * pe = p + value.size();
	while (p < pe) {
		uuid_t inode;
		int size;
		std::string name;

		char type = *p++;

		if (!(type == 'd' || type == 'f')) {
			fprintf(l, "unknown type %c\n", type);
			return false;
		}

		fprintf(l, "read entry of type %c\n", type);
		
		memcpy(inode, p, sizeof(inode));
		p += sizeof(inode);
		memcpy(&size, p, sizeof(size));
		p += sizeof(size);
		name.reserve(size);
		name.insert(name.end(), p, p + size);
		p += size;

		fprintf(l, "read entry of name '%s'\n", name.c_str());

		switch (type) {
		case 'd':
		{
			dentry * d = new dentry(name);
			memcpy(d->inode, inode, sizeof(inode)); // TODO: ugly
			if (!d->read()) {
				// TODO: error
				fprintf(l, "cannot read child entry %s\n", name.c_str());
				return false;
			}
			entries[name] = boost::shared_ptr<entry>(d);
			
			break;
		}
		case 'f':
		{
			fentry * f = new fentry(name);
			memcpy(f->inode, inode, sizeof(inode)); // TODO: ugly
			entries[name] = boost::shared_ptr<entry>(f);
			break;
		}
		default:
			// TODO: error;
			fprintf(l, "unknown error\n");
			return false;
		}
	}

	return true;
}

std::string entry::row()
{
	std::string ret;
	std::string k = key();
	ret.insert(ret.end(), k.begin(), k.end());
	
	int size = name.size();
	ret.insert(ret.end(), (char*)&size, (char*)&size + sizeof(size));
	ret.insert(ret.end(), name.begin(), name.end());
	return ret;
}

void fentry::write(leveldb::WriteBatch & batch)
{
	std::string value;
	value.insert(value.end(), (char*)&size, (char*)&size + sizeof(size));
	batch.Put(key(), value);
}

std::string fentry::key()
{
	char k[sizeof(inode)+1];
	k[0] = 'f';
	memcpy(&k[1], inode, sizeof(inode));
	return std::string(k, sizeof(k));
}

std::string dentry::key()
{
	char k[sizeof(inode)+1];
	k[0] = 'd';
	memcpy(&k[1], inode, sizeof(inode));
	return std::string(k, sizeof(k));
}

void dentry::write(leveldb::WriteBatch & batch)
{
	std::string value;

	for (entries_t::iterator it = entries.begin(); it != entries.end(); ++it) {
		value += it->second->row();
	}

	fprintf(l, "writing dentry '%s' -> %lu\n", name.c_str(), value.size());

	batch.Put(key(), value);
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
	s->st_mode = S_IFREG | 0666;
	s->st_nlink = 1;
	s->st_size = size;
	memcpy(&s->st_ino, inode, sizeof(s->st_ino)); 
}

void dentry::fillstat(struct stat * s)
{
	s->st_mode = S_IFDIR | 0755;
	s->st_nlink = 2;
	s->st_size = 4096;
	memcpy(&s->st_ino, inode, sizeof(s->st_ino)); 
}
