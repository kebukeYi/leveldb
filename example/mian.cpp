//
// Created by 19327 on 2025/5/09/星期五.
//

#include <iostream>
#include "util/random.h"
#include "include/leveldb/db.h"

void insertData(leveldb::DB* db, u_int64_t size);

int main() {
  leveldb::DB* db;
  leveldb::Options options;// 默认执行空构造方法;
  options.create_if_missing = true;
  // 打开数据库;
  leveldb::Status status = leveldb::DB::Open(options, "/usr/cliondata/leveldbdata", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return -1;
  }

  // insertData(db,126550);

  // assert(status.ok());
  std::cout << "leveldb open success!" << std::endl;

  std::string value;
  std::string key1 = "testkey1";
  //
  leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
  if (s.IsNotFound()) {
    std::cout << "can not found for key:" << key1 << std::endl;
    db->Put(leveldb::WriteOptions(), key1, "testvalue1");
  }
  //
  s = db->Get(leveldb::ReadOptions(), key1, &value);
  if (s.ok()) {
    std::cout << "found key:" << key1 << ",value:" << value << std::endl;
  }
  //
  s = db->Delete(leveldb::WriteOptions(), key1);
  if (s.ok()) {
    std::cout << "delete key success which key:" << key1 << std::endl;
  }
  //
  s = db->Get(leveldb::ReadOptions(), key1, &value);
  if (s.IsNotFound()) {
    std::cout << "can not found after delete for key:" << key1 << std::endl;
  }

  delete db;
  return 0;
}

std::string RandomString(leveldb::Random* rnd, int len) {
  std::string r;
  r.resize(len);
  for (int i = 0; i < len; i++) {
    r[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
  }
  return r;
}

std::string RandomKey(leveldb::Random* rnd) {
  int len =(rnd->OneIn(3) ? 1 : (rnd->OneIn(100) ? rnd->Skewed(10) : rnd->Uniform(10)));

  // Make sure to generate a wide variety of characters so we
  // test the boundary conditions for short-key optimizations.
  static const char kTestChars[] = {'\0', '\1', 'a',    'b',    'c',
                                    'd',  'e',  '\xfd', '\xfe', '\xff'};
  std::string result;
  for (int i = 0; i < len; i++) {
    result += kTestChars[rnd->Uniform(sizeof(kTestChars))];
  }
  return result;
}


void insertData(leveldb::DB* db, u_int64_t size) {
  leveldb::Random rnd(rand());
  for (int i = 0; i < size; i++) {
    int p = rnd.Uniform(100);
    if (p < 45) {  // Put
      std::string k;
      std::string v;
      k = RandomKey(&rnd);
      v = RandomString(&rnd,rnd.OneIn(20) ? 100 + rnd.Uniform(100) : rnd.Uniform(8));
      db->Put(leveldb::WriteOptions(), k, v);
    }
  }
}
