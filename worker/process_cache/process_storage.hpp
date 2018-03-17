#pragma once

#include <functional>
#include <boost/noncopyable.hpp>
#include <base/third_party/sarray.h>
#include "base/magic.hpp"

namespace flexps {

template <typename Val>
class ProcessStorage {
public:
  struct ChunkInfo {
    third_party::SArray<Val> chunk; 
    int clock;
  };
  ProcessStorage(){};

  ~ProcessStorage(){};

  //template <typename V>
  third_party::SArray<Val> Find(Key chunk_id);

  //template <typename V>
  bool Insert(Key chunk_id, std::vector<Val>& chunk, int clock);

  //template <typename V>
  third_party::SArray<Key> FindChunkToUpdate(third_party::SArray<Key> chunk_ids, int min_clock);
private:
  //chunk_id -> clock
  std::map<int, int> chunks_clock_;
  std::map<int, ChunkInfo> cached_kvs_;
  uint32_t chunk_size_;
};

  template <typename Val>
  third_party::SArray<Val> ProcessStorage<Val>::Find(Key chunk_id) {
    if ( cached_kvs_.find(chunk_id) == cached_kvs_.end() ) {
      return third_party::SArray<Val> ();
    } else {
      return cached_kvs_[chunk_id].chunk;
    }
  }
  
  template <typename Val>
  bool ProcessStorage<Val>::Insert(Key chunk_id, std::vector<Val>& chunk, int clock) {
    if (cached_kvs_.find(chunk_id) == cached_kvs_.end() || cached_kvs_[chunk_id].clock < clock) {
      ChunkInfo chunk_info;
      chunk_info.chunk =  third_party::SArray<Val>(chunk);
      chunk_info.clock = clock;
      cached_kvs_[chunk_id] = chunk_info;
      chunks_clock_[chunk_id] = clock;
      return true;
    } else {
      return false;
    }
  }

  template <typename Val>
  third_party::SArray<Key> ProcessStorage<Val>::FindChunkToUpdate(third_party::SArray<Key> chunk_ids, int min_clock) {
    third_party::SArray<Key> chunk_to_update;
    for (auto chunk_id : chunk_ids)
    {
      if (chunks_clock_.find(chunk_id) == chunks_clock_.end() || chunks_clock_[chunk_id] < min_clock)
        chunk_to_update.push_back(chunk_id);
    }
/*    for(auto pair : chunks_clock_ ) {
      if( pair.second < min_clock )
          chunk_to_update.push_back(pair.first);
    }
*/
    return chunk_to_update;

  }
}  // namespace flexps
