#include "gtest/gtest.h"

#include <future>
#include <thread>
#include "glog/logging.h"

#include "worker/process_cache/cached_blocker.hpp"
#include "worker/process_cache/process_storage.hpp"

namespace flexps {
namespace {

class TestProcessCache : public testing::Test {
 public:
  TestProcessCache() {}
  ~TestProcessCache() {}

 protected:
  void SetUp() {}
  void TearDown() {}
};

TEST_F(TestProcessCache, InsertAndFind) {
  LOG(INFO) << "Hi!";
  third_party::SArray<Key> keys{3,4,5,10};
  std::vector<float> chunk_1(10, 0.1);
  std::vector<float> chunk_2(10, 0.2);
  std::vector<float> chunk_3(10, 0.3);
  std::vector<float> chunk_4(10, 0.4);
  std::vector<std::vector<float>> chunks = {chunk_1, chunk_2, chunk_3, chunk_4};
  int clock[4] = {0, 1, 2, 3};
   
  LOG(INFO) << "Insert into Process Storage";
  ProcessStorage<float> process_storage;
  for (int i = 0; i < 4; i++)
  {
    process_storage.Insert(keys[i], chunks[i], clock[i]);
  }
  
  LOG(INFO) << "Find";
  std::vector<float> expected_chunk_1(10, 0.1);
  std::vector<float> expected_chunk_2(10, 0.2);
  std::vector<float> expected_chunk_3(10, 0.3);
  std::vector<float> expected_chunk_4(10, 0.4);
  std::vector<std::vector<float>> expected_chunks = {expected_chunk_1, expected_chunk_2, expected_chunk_3, expected_chunk_4};
  for (int i = 0; i < 4; i++)
  {  
    std::vector<float> test_val(10); 
    third_party::SArray<float> vals_in_process_storage = process_storage.Find(keys[i]);
    memcpy(test_val.data(), vals_in_process_storage.data(), 10*sizeof(float));
    EXPECT_EQ(test_val, expected_chunks[i]);  
  }
  LOG(INFO) << "Finished";
}

TEST_F(TestProcessCache, FindChunkToUpdate){
  LOG(INFO) << "Hi!";
  third_party::SArray<Key> keys{3,4,5,10};
  std::vector<float> chunk_1(10, 0.1);
  std::vector<float> chunk_2(10, 0.2);
  std::vector<float> chunk_3(10, 0.3);
  std::vector<float> chunk_4(10, 0.4);
  std::vector<std::vector<float>> chunks = {chunk_1, chunk_2, chunk_3, chunk_4};
  int clock[4] = {0, 1, 2, 3};
   
  LOG(INFO) << "Insert into Process Storage";
  ProcessStorage<float> process_storage;
  for (int i = 0; i < 4; i++)
  {
    process_storage.Insert(keys[i], chunks[i], clock[i]);
  }
  
  LOG(INFO) << "FindChunkToUpdate";
  int check_clock[5] = {0, 1, 2, 3, 4};
  
  std::vector<Key> expected_keys_1{3, 4, 5, 10}; 
  //std::vector<Key> expected_keys_2{3, 4, 5}; 
  //std::vector<Key> expected_keys_3{3, 4}; 
  //std::vector<Key> expected_keys_4{3}; 
  for (int i = 0; i < 5; i++){
    third_party::SArray<Key> request_keys = {3, 4, 5, 10, 11}; 
    LOG(INFO) << "check_clock: " << check_clock[i];
    third_party::SArray<Key> return_keys = process_storage.FindChunkToUpdate(request_keys, check_clock[i]);

    CHECK_EQ(return_keys.size(), i+1);
    CHECK_EQ(return_keys[return_keys.size() - 1], 11);
    for(int j = 0; j < i; j++)
    {
       CHECK_EQ(return_keys[j], request_keys[j]);
    } 
  } 
}

TEST_F(TestProcessCache, Construct) { CachedBlocker blocker; }

TEST_F(TestProcessCache, Register) {
  ThreadsafeQueue<Message> send_queue;
  ProcessStorage<float> process_storage;  
  Message m;
  auto f1 = [m](Message& message){};
  auto f2 = [](){};

  CachedBlocker blocker;
  blocker.RegisterProcessStorage(&process_storage);
  blocker.RegisterSenderqueue(&send_queue);
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
}

TEST_F(TestProcessCache, GetReply) {
  ThreadsafeQueue<Message> send_queue;
  ProcessStorage<float> process_storage;  
  CachedBlocker blocker;
  int f1_counter = 0;
  int f2_counter = 0;
  Message m;
  m.meta.sender = 237;  // set the sender to check whether the messages are the same
  m.meta.flag = Flag::kGetReply;
  m.meta.version = 0;
  third_party::SArray<Key> k{1};
  third_party::SArray<float> v{0.4};
  m.AddData(k);
  m.AddData(v); 
  auto f1 = [&f1_counter, m](Message& message) {
    f1_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  auto f2 = [&f2_counter]() { f2_counter += 1; };
  blocker.RegisterProcessStorage(&process_storage);
  blocker.RegisterSenderqueue(&send_queue);
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
  blocker.NewRequest(0, 0, 2);
  blocker.AddResponse(0, 0, m);
  EXPECT_EQ(f1_counter, 1);
  EXPECT_EQ(f2_counter, 0);
  blocker.AddResponse(0, 0, m);
  EXPECT_EQ(f1_counter, 2);
  EXPECT_EQ(f2_counter, 1);
}

TEST_F(TestProcessCache, GetChunkReply) {
  ProcessStorage<float> process_storage; 
  CachedBlocker blocker;
  ThreadsafeQueue<Message> send_queue;
  int f1_counter = 0;
  int f2_counter = 0;
  Message m;
  m.meta.sender = 237;  // set the sender to check whether the messages are the same
  m.meta.flag = Flag::kGetChunkReply;
  m.meta.version = 0;
  
  std::vector<Key> keys = {3, 4, 5, 6};
  std::vector<float> val1 (10, 0.1);
  std::vector<float> val2 (10, 0.2); 
  std::vector<float> val3 (10, 0.3); 
  std::vector<float> val4 (10, 0.4); 
  std::vector<std::vector<float>> vals = {val1,val2,val3,val4};

  int key_size = keys.size();
  int chunk_size = vals[0].size();
  std::vector<float> chunk_vals(key_size * chunk_size);
  for (int i = 0; i < key_size; i++){
    for (int j = 0; j < chunk_size; j++){
      chunk_vals[i * chunk_size + j] = vals[i][j];
    }
  }

  m.AddData(third_party::SArray<Key>(keys));
  m.AddData(third_party::SArray<float>(chunk_vals));
  LOG(INFO) << "Hi!"; 
  auto f1 = [m](Message& message) {};
  auto f2 = [&f2_counter]() {};

  blocker.RegisterProcessStorage(&process_storage);
  blocker.RegisterSenderqueue(&send_queue);
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
  blocker.NewRequest(0, 0, 2);
  blocker.AddResponse(0, 0, m);
  blocker.AddResponse(0, 0, m);

  for (int v = 0; v < 4; v++)
  {
    std::vector<std::vector<float>*> check_vals(4);
    std::vector<float> expected_val (10, (float)(v+1)/10);
    std::vector<float> test_val(10);
    third_party::SArray<float> vals_in_process_storage = process_storage.Find(3+v);
    memcpy(test_val.data(), vals_in_process_storage.data(), 10*sizeof(float));
    EXPECT_EQ(test_val, expected_val); 
  }
}

TEST_F(TestProcessCache, GetChunk) {
  int sender = 237;
  int recver = 961;
  ProcessStorage<float> process_storage; 
  ThreadsafeQueue<Message> send_queue;
  CachedBlocker blocker;
  Message clock_message; // for setup clock in advance
  clock_message.meta.sender = sender;
  clock_message.meta.recver = recver;
  clock_message.meta.flag = Flag::kClock;
  clock_message.meta.model_id = 0;
  auto f1 = [](Message& message) {};
  auto f2 = [&]() {

  };

  third_party::SArray<Key> keys{3,4,5,10};
  std::vector<float> chunk_1(10, 0.1);
  std::vector<float> chunk_2(10, 0.2);
  std::vector<float> chunk_3(10, 0.3);
  std::vector<float> chunk_4(10, 0.4);
  std::vector<std::vector<float>> chunks = {chunk_1, chunk_2, chunk_3, chunk_4};
  int clock[4] = {0, 1, 2, 3};
   
  LOG(INFO) << "Insert into Process Storage";
  for (int i = 0; i < 4; i++)
  {
    process_storage.Insert(keys[i], chunks[i], clock[i]);
  }  

  blocker.RegisterProcessStorage(&process_storage);
  blocker.RegisterSenderqueue(&send_queue);

  // set the clock to 4 for testing
  Message s;
  blocker.AddResponse(sender, 0, clock_message);
  send_queue.WaitAndPop(&s);
  blocker.AddResponse(sender, 0, clock_message);
  send_queue.WaitAndPop(&s);
  blocker.AddResponse(sender, 0, clock_message);
  send_queue.WaitAndPop(&s);
  blocker.AddResponse(sender, 0, clock_message);
  send_queue.WaitAndPop(&s);

  std::thread th([&]() {
    Message get_chunk_message;
    get_chunk_message.meta.sender = sender;  // set the sender to check whether the messages are the same
    get_chunk_message.meta.recver = recver;  // set the recver to check whether the messages are the same
    get_chunk_message.meta.flag = Flag::kGetChunk;
    //m.meta.version = 0;
    third_party::SArray<Key> request_keys{3, 4, 5, 10};
    
    //third_party::SArray<float> v{0.4};
    get_chunk_message.AddData(request_keys);
    //m.AddData(v); 
    blocker.RegisterRecvHandle(sender, 0, f1);
    blocker.RegisterRecvFinishHandle(sender, 0, f2);
    blocker.NewChunkRequest(sender, 0, request_keys.size());
    blocker.AddResponse(sender, 0, get_chunk_message);
    blocker.WaitChunkRequest(sender, 0);  
  });
  
  LOG(INFO) << "send_keys";
  send_queue.WaitAndPop(&s);
  third_party::SArray<Key> send_keys;
  
  send_keys = s.data[0];
  LOG(INFO) << "send_keys.size(): " << send_keys.size();
  LOG(INFO) << send_keys[0];
  EXPECT_EQ(send_keys[0], keys[0]); 
  
  Message get_chunk_reply_message_1;
  get_chunk_reply_message_1.meta.sender = recver; 
  get_chunk_reply_message_1.meta.recver = sender;
  get_chunk_reply_message_1.meta.flag = Flag::kGetChunkReply; 
  get_chunk_reply_message_1.meta.version = 3;
  std::vector<Key> reply_keys_1{3, 4, 5, 10};
  std::vector<float> reply_vals_1;
  for (int i = 0; i < 40; i++) reply_vals_1.push_back(((20+i)/10)*0.1);
  get_chunk_reply_message_1.AddData(third_party::SArray<Key> (reply_keys_1)); 
  get_chunk_reply_message_1.AddData(third_party::SArray<float> (reply_vals_1)); 
  blocker.AddResponse(sender, 0, get_chunk_reply_message_1);

  Message get_chunk_reply_message_2;
  get_chunk_reply_message_2.meta.sender = recver; 
  get_chunk_reply_message_2.meta.recver = sender;
  get_chunk_reply_message_2.meta.flag = Flag::kGetChunkReply; 
  get_chunk_reply_message_2.meta.version = 4;
  std::vector<Key> reply_keys_2{3,4};
  std::vector<float> reply_vals_2;
  for (int i = 0; i < 20; i++) reply_vals_1.push_back(((20+i)/10)*0.1);
  get_chunk_reply_message_2.AddData(third_party::SArray<Key> (reply_keys_2)); 
  get_chunk_reply_message_2.AddData(third_party::SArray<float> (reply_vals_2)); 
  blocker.AddResponse(sender, 0, get_chunk_reply_message_2);

  Message get_chunk_reply_message_3;
  get_chunk_reply_message_3.meta.sender = recver; 
  get_chunk_reply_message_3.meta.recver = 263;
  get_chunk_reply_message_3.meta.flag = Flag::kGetChunkReply; 
  get_chunk_reply_message_3.meta.version = 4;
  std::vector<Key> reply_keys_3{5, 10};
  std::vector<float> reply_vals_3;
  for (int i = 0; i < 20; i++) reply_vals_3.push_back(((20+i)/10)*0.1);
  get_chunk_reply_message_3.AddData(third_party::SArray<Key> (reply_keys_3)); 
  get_chunk_reply_message_3.AddData(third_party::SArray<float> (reply_vals_3)); 
  blocker.AddResponse(sender, 0, get_chunk_reply_message_3);

  th.join();
}

TEST_F(TestProcessCache, Clock) {
  CachedBlocker blocker;
  ThreadsafeQueue<Message> send_queue;
  Message m;
  m.meta.sender = 237;  // set the sender to check whether the messages are the same
  m.meta.flag = Flag::kClock;
  m.meta.version = 10;
  auto f1 = [](Message& message) {};
  auto f2 = []() {};
  blocker.RegisterSenderqueue(&send_queue);
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
  blocker.NewRequest(0, 0, 1);
  blocker.AddResponse(0, 0, m);
  Message s;
  send_queue.WaitAndPop(&s);
  EXPECT_EQ(s.meta.version, 10);
}


TEST_F(TestProcessCache, MultiThreads) {
  CachedBlocker blocker;
  int f1_counter = 0;
  int f2_counter = 0;
  Message m;
  m.meta.sender = 237;  // set the sender to check whether the messages are the same
  m.meta.flag = Flag::kGetReply;
  m.meta.version = 0;
  third_party::SArray<Key> k{1};
  third_party::SArray<float> v{0.4};
  m.AddData(k);
  m.AddData(v);  
  auto f1 = [&f1_counter, m](Message& message) {
    f1_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  auto f2 = [&f2_counter]() { f2_counter += 1; };
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
  std::promise<void> prom;
  std::future<void> fut = prom.get_future();
  std::thread th([&blocker, &prom] {
    blocker.NewRequest(0, 0, 2);
    prom.set_value();
    blocker.WaitRequest(0, 0);
  });
  fut.get();
  blocker.AddResponse(0, 0, m);
  EXPECT_EQ(f1_counter, 1);
  EXPECT_EQ(f2_counter, 0);
  blocker.AddResponse(0, 0, m);
  EXPECT_EQ(f1_counter, 2);
  EXPECT_EQ(f2_counter, 1);
  th.join();
}

TEST_F(TestProcessCache, MultiThreadsReused) {
  CachedBlocker blocker;
  int f1_counter = 0;
  int f2_counter = 0;
  Message m;
  m.meta.sender = 237;  // set the sender to check whether the messages are the same
  m.meta.flag = Flag::kGetReply;
  m.meta.version = 0;
  third_party::SArray<Key> k{1};
  third_party::SArray<float> v{0.4};
  m.AddData(k);
  m.AddData(v);  
  auto f1 = [&f1_counter, m](Message& message) {
    f1_counter += 1;
    EXPECT_EQ(m.meta.sender, message.meta.sender);
  };
  auto f2 = [&f2_counter]() { f2_counter += 1; };
  blocker.RegisterRecvHandle(0, 0, f1);
  blocker.RegisterRecvFinishHandle(0, 0, f2);
  {
    std::promise<void> prom;
    std::future<void> fut = prom.get_future();
    std::thread th([&blocker, &prom] {
      blocker.NewRequest(0, 0, 2);
      prom.set_value();
      blocker.WaitRequest(0, 0);
    });
    fut.get();
    blocker.AddResponse(0, 0, m);
    EXPECT_EQ(f1_counter, 1);
    EXPECT_EQ(f2_counter, 0);
    blocker.AddResponse(0, 0, m);
    EXPECT_EQ(f1_counter, 2);
    EXPECT_EQ(f2_counter, 1);
    th.join();
  }
  // Second round
  {
    f1_counter = 0;
    f2_counter = 0;
    std::promise<void> prom;
    std::future<void> fut = prom.get_future();
    std::thread th([&blocker, &prom] {
      blocker.NewRequest(0, 0, 2);
      prom.set_value();
      blocker.WaitRequest(0, 0);
    });
    fut.get();
    blocker.AddResponse(0, 0, m);
    EXPECT_EQ(f1_counter, 1);
    EXPECT_EQ(f2_counter, 0);
    blocker.AddResponse(0, 0, m);
    EXPECT_EQ(f1_counter, 2);
    EXPECT_EQ(f2_counter, 1);
    th.join();
  }
}

}  // namespace
}  // namespace flexps
