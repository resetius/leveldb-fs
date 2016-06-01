#include <string>
#include <stdlib.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/filter_policy.h"
#include "leveldb/env.h"

leveldb::DB* db;
int blocksize = 10000;

void * writer(void * a)
{
	long number = (long)a;

	char data[blocksize];

	for (int j = 0; j < blocksize; ++j) {
		data[j] = (char)(rand() % 256);
	}

	size_t written = 0;

	int batchc = 10;

	char key[batchc][256];
	long block = 0;
	std::string value(data, blocksize);

	leveldb::WriteOptions writeOptions;
	writeOptions.sync = false;

	while (1) {
//		snprintf(key, sizeof(key), "%04ld%04ld", number, block);
//		db->Put(writeOptions, key, value);
//		block++;
		leveldb::WriteBatch batch;
		for (int k = 0; k < batchc; ++k) {
			snprintf(key[k], 256, "%04ld%04ld", number, block);
			//db->Put(writeOptions, key, value);
			batch.Put(key[k], value);
			block++;
		}
		db->Write(writeOptions, &batch);

	}

	return 0;
}

int main(int argc, char ** argv)
{
	int threads = 50;
	leveldb::Options options;

	options.create_if_missing = true;
	options.compression = leveldb::kNoCompression;
//	options.write_buffer_size = 4*1024*1024;
//
	options.filter_policy=leveldb::NewBloomFilterPolicy2(16);
    options.write_buffer_size=62914560;  // 60Mbytes
    options.total_leveldb_mem=2684354560; // 2.5Gbytes (details below)
	options.env=leveldb::Env::Default();

	leveldb::Status status = leveldb::DB::Open(options, "./testdb", &db);

//	db->CompactRange(0, 0);
//	return 0;

	pthread_t t[threads];

	for (long i = 0; i < threads; ++i) {
		pthread_create(&t[i], 0, writer, (void*)i);
	}

	for (int i = 0; i < threads; ++i) {
		pthread_join(t[i], 0);
	}

	return 0;
}


