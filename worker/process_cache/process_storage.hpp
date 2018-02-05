#pragma once

#include <functional>
#include <boost/noncopyable.hpp>
#include <base/third_party/sarray.h>
#include <base/magic.hpp>

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

  std::vector<Val> Find(int32_t chunk_id);
  bool Insert(third_party::SArray<Key> chunk_ids, third_party::SArray<Val> chunk, int32_t clock){return true;};
  //std::vector<int32_t> FindChunkToUpdate(std::vector<int> chunk_ids, int min_clock){return chunk_ids;};
  third_party::SArray<Key> FindChunkToUpdate(third_party::SArray<Key> chunk_ids, int min_clock){return chunk_ids;};
private:
  //chunk_id -> clock
  std::map<int, int> chunks_clock;
  std::map<int, ChunkInfo> cached_kvs;
  uint32_t chunk_size_;
};

}  // namespace flexps
