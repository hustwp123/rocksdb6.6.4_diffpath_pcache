//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/filter_policy.h"

#include <array>
#include <tries/path_decomposed_trie.hpp>
#include <tries/vbyte_string_pool.hpp>
#include <time.h>

#include "compacted_trie_builder.hpp"
#include "rocksdb/slice.h"
#include "succinct/bp_vector.hpp"
#include "succinct/elias_fano.hpp"
#include "succinct/forward_enumerator.hpp"
#include "table/block_based/block_based_filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "third-party/folly/folly/ConstexprMath.h"
#include "util/bloom_impl.h"
#include "util/coding.h"
#include "util/hash.h"

namespace rocksdb {

namespace {


// xp
class OtLexPdtBloomBitsBuilder : public BuiltinFilterBitsBuilder {
 public:
  explicit OtLexPdtBloomBitsBuilder()
      :ot_pdt() {
    //fprintf(stderr, "constructing OtLexPdtBloomBitsBuilder()\n");
  }

  // No Copy allowed
  OtLexPdtBloomBitsBuilder(const OtLexPdtBloomBitsBuilder&) = delete;
  void operator=(const OtLexPdtBloomBitsBuilder&) = delete;

  ~OtLexPdtBloomBitsBuilder() override {}

  virtual void AddKey(const Slice& key) override {
//    fprintf(stderr, "in OtLexPdtBloomBitsBuilder::AddKey() idpaeq\n");
    std::string key_string(key.data(), key.data()+key.size());
    key_strings_.push_back(key_string);
  }

  uint32_t CalculateSpace(const int num_entry) override {
    return static_cast<uint32_t>(CalculateByteSpace());
  }

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
//    fprintf(stderr, "in OtLexPdtBloomBitsBuilder::Finish() 8qpeye\n");
    // generate a compacted trie and get essential data
    assert(key_strings_.size() > 0);
    key_strings_.erase(unique(key_strings_.begin(),
                              key_strings_.end()),
                       key_strings_.end()); //xp, for now simply dedup keys

//    fprintf(stdout, "DEBUG w7zvbg key_strings_.size: %lu\n", key_strings_.size());

    // auto chrono_start = std::chrono::system_clock::now();
    ot_pdt.construct_compacted_trie(key_strings_, false); // ot_pdt.pub_ are inited
    // auto chrono_end = std::chrono::system_clock::now();
    // std::chrono::microseconds elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(chrono_end-chrono_start);
    // std::cout << "DEBUG 3yb6fo Finis() ot_pdt use " << key_strings_.size() << " keys build raw trie takes(us) " <<
              // elapsed_us.count() << std::endl;

    // get the byte size of key_strings_
    uint64_t ot_lex_pdt_byte_size = CalculateByteSpace();
    assert(ot_lex_pdt_byte_size > 0);
    uint64_t buf_byte_size = ot_lex_pdt_byte_size + 5;
//    fprintf(stderr, "DEBUG 10dk3 compacted trie byte size: %lu\n", buf_byte_size);

    char* contents = new char[buf_byte_size];
    memset(contents, 0, buf_byte_size);
    assert(contents);

    //xp, be compatible with new bloom filter (full filter)
    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -1 = Marker for newer Bloom implementations
    char new_impl = static_cast<char>(-1);
//    contents[byte_size] = static_cast<char>(-1);
    // 0 = Marker for this sub-implementation, ot lex pdt
    char sub_impl = static_cast<char>(80); // 'P'
//    contents[byte_size+1] = static_cast<char>(5);
    // just for padding
    // when full filter: num_probes (and 0 in upper bits for 64-byte block size)
    char fake_num_probes = static_cast<char>(7);
//    contents[byte_size+2] = static_cast<char>(7);
    // rest of metadata (2 chars) are paddings

    // format essential data with format
    // vec<uint16_t>,
    // vec<uint16_t>,
    // vec<uint8_t>,
    // vec<uint8_t>,
    // vec<uint64_t>,
    // uint64_t
    // char
    // char
    // char
    // & ot lex pdt byte size
    // target buffer
//    fprintf(stdout, "DEBUG a3gcn6 in otFinish sizes for string,label,branch,char,bit,size:%ld,%ld,%ld,%ld,%ld,%ld\n",
//            ot_pdt.pub_m_centroid_path_string.size(),
//            ot_pdt.pub_m_labels.size(),
//            ot_pdt.pub_m_centroid_path_branches.size(),
//            ot_pdt.pub_m_branching_chars.size(),
//            ot_pdt.pub_m_bp_m_bits.size(),
//            ot_pdt.pub_m_bp_m_size);
    PutIntoCharArray(ot_pdt.pub_m_centroid_path_string,
                     ot_pdt.pub_m_labels,
                     ot_pdt.pub_m_centroid_path_branches,
                     ot_pdt.pub_m_branching_chars,
                     ot_pdt.pub_m_bp_m_bits,
                     ot_pdt.pub_m_bp_m_size,
                     new_impl,
                     sub_impl,
                     fake_num_probes,
                     buf_byte_size,
                     contents);

//    for (size_t i = 0; i < ot_pdt.pub_m_centroid_path_string.size(); i++) {
//      fprintf(stdout, "Pstring:%ld,%d\n", i, ot_pdt.pub_m_centroid_path_string[i]);
//    }
//    for (size_t i = 0; i < ot_pdt.pub_m_labels.size(); i++) {
//      fprintf(stdout, "Plabel:%ld,%d\n", i, ot_pdt.pub_m_labels[i]);
//    }
//    for (size_t i = 0; i < ot_pdt.pub_m_centroid_path_branches.size(); i++) {
//      fprintf(stdout, "Pbranch:%ld,%d\n", i, ot_pdt.pub_m_centroid_path_branches[i]);
//    }
//    for (size_t i = 0; i < ot_pdt.pub_m_branching_chars.size(); i++) {
//      fprintf(stdout, "Pchar:%ld,%d\n", i, ot_pdt.pub_m_branching_chars[i]);
//    }
//    for (size_t i = 0; i < ot_pdt.pub_m_bp_m_bits.size(); i++) {
//      fprintf(stdout, "Pbit:%ld,%ld\n", i, ot_pdt.pub_m_bp_m_bits[i]);
//    }

    assert(sizeof(ot_pdt.pub_m_bp_m_size) != 0);

    // return a Slice with data and its byte length
    const char* const_data = contents;
    buf->reset(const_data);
    key_strings_.clear();
    return Slice(contents, buf_byte_size);
//=============
//    uint32_t len_with_metadata =
//        CalculateSpace(static_cast<uint32_t>(hash_entries_.size()));
//    char* data = new char[len_with_metadata];
//    memset(data, 0, len_with_metadata);
//
//    assert(data);
//    assert(len_with_metadata >= 5);
//
//    uint32_t len = len_with_metadata - 5;
//    if (len > 0) {
//      AddAllEntries(data, len);
//    }

    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -1 = Marker for newer Bloom implementations
//    data[len] = static_cast<char>(-1);
    // 0 = Marker for this sub-implementation
//    data[len + 1] = static_cast<char>(0);
    // num_probes (and 0 in upper bits for 64-byte block size)
//    data[len + 2] = static_cast<char>(num_probes_);
    // rest of metadata stays zero

//    const char* const_data = data;
//    buf->reset(const_data);
//    hash_entries_.clear();
//    return Slice(data, len_with_metadata);
  }

  // ot lex pdt used byte size
  uint64_t CalculateByteSpace() {
    // calculate the byte size of format buf
    return (ot_pdt.pub_m_centroid_path_string.size()+ot_pdt.pub_m_labels.size())*2 +
        ot_pdt.pub_m_centroid_path_branches.size()+ ot_pdt.pub_m_branching_chars.size() +
        ot_pdt.pub_m_bp_m_bits.size() * 8 + 8 +
        5 * 4; // sizes of above vectors
  }

  // put essential data into char[], and this char[] will be stored in the Slice
  // of a particular Table.
  //  NOTE: total used byte == byte_size (ot lex pdt byte size) + 5
  char* PutIntoCharArray(std::vector<uint16_t>& v1, std::vector<uint16_t>& v2,
                         std::vector<uint8_t>& v3, std::vector<uint8_t>& v4,
                         std::vector<uint64_t>& v5, uint64_t num,
                         char new_impl, char sub_impl, char fake_num_probes,
                         uint64_t& byte_size, char*& buf) {
//    byte_size =
//        (v1.size() + v2.size()) * 2 + v3.size() + v4.size() + v5.size() * 8 + 8;
//    byte_size += 5 * 4 + 5;  //指明4个vector的size + 5 padding chars
//
    buf = new char[byte_size];
    memset(buf, 0, byte_size);

    uint32_t* p = (uint32_t*)buf;
    *p = v1.size();
    uint16_t* p1 = (uint16_t*)(buf + 4);
    for (uint32_t i = 0; i < v1.size(); i++) {
      *p1 = v1[i];
      p1++;
    }

    p = (uint32_t*)(buf + 4 + v1.size() * 2);
    *p = v2.size();
    p1 = (uint16_t*)(buf + 4 + v1.size() * 2 + 4);
    for (uint32_t i = 0; i < v2.size(); i++) {
      *p1 = v2[i];
      p1++;
    }

    p = (uint32_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2);
    *p = v3.size();
    uint8_t* p2 = (uint8_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4);
    for (uint32_t i = 0; i < v3.size(); i++) {
      *p2 = v3[i];
      p2++;
    }

    p = (uint32_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                    v3.size());
    *p = v4.size();
    p2 = (uint8_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                    v3.size() + 4);
    for (uint32_t i = 0; i < v4.size(); i++) {
      *p2 = v4[i];
      p2++;
    }

    p = (uint32_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                    v3.size() + 4 + v4.size());
    *p = v5.size();
    fprintf(stderr, "DEBUG h8qd8z PutIntoCharArray m_bits.size(): %u\n", *p);
    uint64_t* p4 = (uint64_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                               v3.size() + 4 + v4.size() + 4);
    for (uint32_t i = 0; i < v5.size(); i++) {
      *p4 = v5[i];
      p4++;
    }

    uint64_t* p3 = (uint64_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                               v3.size() + 4 + v4.size() + 4 + v5.size() * 8);
    *p3 = num;
//    fprintf(stderr, "DEBUG yz92dt PutIntoCharArray num: %lu, *p3:%lu\n", num, *p3);

    // new bloom filter implementation indicators for GetBloomBitsReader
    char* pc1 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8);
    *pc1 = new_impl;
    char* pc2 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8 + 1);
    *pc2 = sub_impl;
    char* pc3 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8 + 1 + 1);
    *pc3 = fake_num_probes;
//    char* pc4 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
//                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8 + 1 + 1 + 1);
//    *pc4 = static_cast<char>(0);
//    char* pc5 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
//                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8 + 1 + 1 + 1 + 1);
//    *pc5 = static_cast<char>(0);

    return buf;
  }


  std::vector<std::string> key_strings_;  // vector for Slice.data

  // a compacted trie, NO need for a ot lex pdt yet
  rocksdb::succinct::tries::path_decomposed_trie<
      rocksdb::succinct::tries::vbyte_string_pool, true>
      ot_pdt;
};

class OtLexPdtBloomBitsReader : public FilterBitsReader {
 public:
  explicit OtLexPdtBloomBitsReader() {
//    fprintf(stderr, "DEBUG pqc7a26 init OtLexPdtBloomBitsReader\n");
  }

  explicit OtLexPdtBloomBitsReader(const char* buf) {
    // construct a ot lex pdt
    // restore essential members from buf
    ot_pdt.pub_m_centroid_path_string.clear();
    ot_pdt.pub_m_labels.clear();
    ot_pdt.pub_m_centroid_path_branches.clear();
    ot_pdt.pub_m_branching_chars.clear();
    ot_pdt.pub_m_bp_m_bits.clear();
    RecoverFromCharArray(ot_pdt.pub_m_centroid_path_string,
                         ot_pdt.pub_m_labels,
                         ot_pdt.pub_m_centroid_path_branches,
                         ot_pdt.pub_m_branching_chars,
                         ot_pdt.pub_m_bp_m_bits,
                         ot_pdt.pub_m_bp_m_size,
                         new_impl,
                         sub_impl,
                         fake_num_probes,
                         buf);
   fprintf(stdout, "DEBUG uq7zbt in otReader sizes for string,label,branch,char,bit,size:%ld,%ld,%ld,%ld,%ld,%ld\n",
           ot_pdt.pub_m_centroid_path_string.size(),
           ot_pdt.pub_m_labels.size(),
           ot_pdt.pub_m_centroid_path_branches.size(),
           ot_pdt.pub_m_branching_chars.size(),
           ot_pdt.pub_m_bp_m_bits.size(),
           ot_pdt.pub_m_bp_m_size);

    //    for (size_t i = 0; i < ot_pdt.pub_m_centroid_path_string.size(); i++) {
//      fprintf(stdout, "Rstring:%ld,%d\n", i, ot_pdt.pub_m_centroid_path_string[i]);
//    }
//    for (size_t i = 0; i < ot_pdt.pub_m_labels.size(); i++) {
//      fprintf(stdout, "Rlabel:%ld,%d\n", i, ot_pdt.pub_m_labels[i]);
//    }
//    for (size_t i = 0; i < ot_pdt.pub_m_centroid_path_branches.size(); i++) {
//      fprintf(stdout, "Rbranch:%ld,%d\n", i, ot_pdt.pub_m_centroid_path_branches[i]);
//    }
//    for (size_t i = 0; i < ot_pdt.pub_m_branching_chars.size(); i++) {
//      fprintf(stdout, "Rchar:%ld,%d\n", i, ot_pdt.pub_m_branching_chars[i]);
//    }
//    for (size_t i = 0; i < ot_pdt.pub_m_bp_m_bits.size(); i++) {
//      fprintf(stdout, "Rbit:%ld,%ld\n", i, ot_pdt.pub_m_bp_m_bits[i]);
//    }

//    fprintf(stderr, "DEBUG c2ys95 after RecoverFromCharArray pub_m_bp_m_size:%lu pub_m_bp_m_bits.size():%lu\n",
//            ot_pdt.pub_m_bp_m_size, ot_pdt.pub_m_bp_m_bits.size());
    // init pub_* members, and create a ot lex pdt instance from it
//        ot_pdt.init_pubs();
//    auto chrono_start = std::chrono::system_clock::now();
    ot_pdt.instance();
//    auto chrono_end = std::chrono::system_clock::now();
//    std::chrono::microseconds elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(chrono_end-chrono_start);
//    std::cout << "DEBUG cor73n ot_pdt.instance() takes " <<
//              elapsed_us.count() << " us." << std::endl;
  }

  // No Copy allowed
  //  OtLexPdtBloomBitsReader(const&) = delete;
  void operator=(const OtLexPdtBloomBitsReader&) = delete;

  ~OtLexPdtBloomBitsReader() override {}

  bool MayMatch(const Slice& key) override {
    // idx = search_odt(key.data)
    // if idx != -1, success
    std::string key_string(key.data(), key.data()+key.size());
//    auto chrono_start = std::chrono::system_clock::now();
    size_t idx = ot_pdt.index(key_string);
//    auto chrono_end = std::chrono::system_clock::now();
//    std::chrono::microseconds elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(chrono_end-chrono_start);
//    std::cout << "DEBUG la045n ot_pdt.index ret:" << idx << ", takes(us) " <<
//              elapsed_us.count() << std::endl;

    //    fprintf(stdout, "DEBUG ab42kf in OtLexPdtBloomBitsReader::MayMatch(%s): %ld\n",
//            key.ToString().c_str(), idx);

//     XXX for test
//    return true;

    if (idx != (size_t)-1) {  // hit
      //??? blk_offset = vector<>.find(idx)
//      fprintf(stdout, "DEBUG i0j5xn ot bitsReader MayMatch(%s): %ld,\n",
//              key.ToString().c_str(), idx);
      return true;
    } else {
      return false;
    }
  }

  virtual void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    fprintf(stdout, "DEBUG w3xm82 in OtBitsReader::MayMatch(num_keys)\n");
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = MayMatch(*keys[i]);
    }
  }

  //wp
  void RecoverFromCharArray(std::vector<uint16_t>& v1,
                            std::vector<uint16_t>& v2,
                            std::vector<uint8_t>& v3,
                            std::vector<uint8_t>& v4,
                            std::vector<uint64_t>& v5,
                            uint64_t& num,
                            char& tmp_new_impl,
                            char& tmp_sub_impl,
                            char& tmp_fake_num_probes,
                            const char* & buf) {
    uint32_t size1 = 0, size2 = 0, size3 = 0, size4 = 0, size5 = 0;

    uint32_t *p = (uint32_t *) buf;
    size1 = *p;
    uint16_t *p1 = (uint16_t *) (buf + 4);
    v1.resize(size1);
    for (uint64_t i = 0; i < size1; i++) {
      v1[i] = *p1;
      p1++;
    }

    p = (uint32_t *) (buf + 4 + size1 * 2);
    size2 = *p;
    p1 = (uint16_t *) (buf + 4 + size1 * 2 + 4);
    v2.resize(size2);
    for (uint64_t i = 0; i < size2; i++) {
      v2[i] = *p1;
      p1++;
    }

    p = (uint32_t *) (buf + 4 + size1 * 2 + 4 + size2 * 2);
    size3 = *p;
    uint8_t *p2 = (uint8_t *) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4);
    v3.resize(size3);
    for (uint64_t i = 0; i < size3; i++) {
      v3[i] = *p2;
      p2++;
    }

    p = (uint32_t *) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3);
    size4 = *p;
    p2 = (uint8_t *) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4);
    v4.resize(size4);
    for (uint64_t i = 0; i < size4; i++) {
      v4[i] = *p2;
      p2++;
    }

    p = (uint32_t *) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4 + size4);
    size5 = *p;
    uint64_t *p4 = (uint64_t *) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4 + size4 + 4);
    v5.resize(size5);
    for (uint64_t i = 0; i < size5; i++) {
      v5[i] = *p4;
      p4++;
    }

    uint64_t *p3 = (uint64_t *) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4 + size4 + 4 + size5 * 8);
    num = *p3;
//    fprintf(stderr, "DEBUG m72qa4 RecoverFromCharArray num: %lu\n", num);

    //xp, be compatible with full filter
    char* pc1 = (char*) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4 + size4 + 4 + size5 * 8 + 8);
    tmp_new_impl = *pc1;
    char* pc2 = (char*) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4 + size4 + 4 + size5 * 8 + 8+1);
    tmp_sub_impl = *pc2;
    char* pc3 = (char*) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4 + size4 + 4 + size5 * 8 + 8+1+1);
    tmp_fake_num_probes = *pc3;
    char* pc4 = (char*) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4 + size4 + 4 + size5 * 8 + 8+1+1+1);
    tmp_fake_num_probes = *pc4;
    char* pc5 = (char*) (buf + 4 + size1 * 2 + 4 + size2 * 2 + 4 + size3 + 4 + size4 + 4 + size5 * 8 + 8+1+1+1+1);
    tmp_fake_num_probes = *pc5;
  }

  // be compatible with full filter in GetBloomBitsReader
  char new_impl;
  char sub_impl;
  char fake_num_probes;
  // a ot lex pdt
  rocksdb::succinct::tries::path_decomposed_trie<
      rocksdb::succinct::tries::vbyte_string_pool, true>
      ot_pdt;
  // a vector<> stores blk boundary keys
  //TODO
};


// See description in FastLocalBloomImpl
class FastLocalBloomBitsBuilder : public BuiltinFilterBitsBuilder {
 public:
  explicit FastLocalBloomBitsBuilder(const int millibits_per_key)
      : millibits_per_key_(millibits_per_key),
        num_probes_(FastLocalBloomImpl::ChooseNumProbes(millibits_per_key_)) {
    assert(millibits_per_key >= 1000);
  }

  // No Copy allowed
  FastLocalBloomBitsBuilder(const FastLocalBloomBitsBuilder&) = delete;
  void operator=(const FastLocalBloomBitsBuilder&) = delete;

  ~FastLocalBloomBitsBuilder() override {}

  virtual void AddKey(const Slice& key) override {
    uint64_t hash = GetSliceHash64(key);
    if (hash_entries_.size() == 0 || hash != hash_entries_.back()) {
      hash_entries_.push_back(hash);
    }
  }

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
    uint32_t len_with_metadata =
        CalculateSpace(static_cast<uint32_t>(hash_entries_.size()));
    char* data = new char[len_with_metadata];
    memset(data, 0, len_with_metadata);

    assert(data);
    assert(len_with_metadata >= 5);

    uint32_t len = len_with_metadata - 5;
    if (len > 0) {
      AddAllEntries(data, len);
    }

    // See BloomFilterPolicy::GetBloomBitsReader re: metadata
    // -1 = Marker for newer Bloom implementations
    data[len] = static_cast<char>(-1);
    // 0 = Marker for this sub-implementation
    data[len + 1] = static_cast<char>(0);
    // num_probes (and 0 in upper bits for 64-byte block size)
    data[len + 2] = static_cast<char>(num_probes_);
    // rest of metadata stays zero

    const char* const_data = data;
    buf->reset(const_data);
    hash_entries_.clear();

    return Slice(data, len_with_metadata);
  }

  int CalculateNumEntry(const uint32_t bytes) override {
    uint32_t bytes_no_meta = bytes >= 5u ? bytes - 5u : 0;
    return static_cast<int>(uint64_t{8000} * bytes_no_meta /
                            millibits_per_key_);
  }

  uint32_t CalculateSpace(const int num_entry) override {
    uint32_t num_cache_lines = 0;
    if (millibits_per_key_ > 0 && num_entry > 0) {
      num_cache_lines = static_cast<uint32_t>(
          (int64_t{num_entry} * millibits_per_key_ + 511999) / 512000);
    }
    return num_cache_lines * 64 + /*metadata*/ 5;
  }

 private:
  void AddAllEntries(char* data, uint32_t len) const {
    // Simple version without prefetching:
    //
    // for (auto h : hash_entries_) {
    //   FastLocalBloomImpl::AddHash(Lower32of64(h), Upper32of64(h), len,
    //                               num_probes_, data);
    // }

    const size_t num_entries = hash_entries_.size();
    constexpr size_t kBufferMask = 7;
    static_assert(((kBufferMask + 1) & kBufferMask) == 0,
                  "Must be power of 2 minus 1");

    std::array<uint32_t, kBufferMask + 1> hashes;
    std::array<uint32_t, kBufferMask + 1> byte_offsets;

    // Prime the buffer
    size_t i = 0;
    for (; i <= kBufferMask && i < num_entries; ++i) {
      uint64_t h = hash_entries_[i];
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len, data,
                                      /*out*/ &byte_offsets[i]);
      hashes[i] = Upper32of64(h);
    }

    // Process and buffer
    for (; i < num_entries; ++i) {
      uint32_t& hash_ref = hashes[i & kBufferMask];
      uint32_t& byte_offset_ref = byte_offsets[i & kBufferMask];
      // Process (add)
      FastLocalBloomImpl::AddHashPrepared(hash_ref, num_probes_,
                                          data + byte_offset_ref);
      // And buffer
      uint64_t h = hash_entries_[i];
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len, data,
                                      /*out*/ &byte_offset_ref);
      hash_ref = Upper32of64(h);
    }

    // Finish processing
    for (i = 0; i <= kBufferMask && i < num_entries; ++i) {
      FastLocalBloomImpl::AddHashPrepared(hashes[i], num_probes_,
                                          data + byte_offsets[i]);
    }
  }

  int millibits_per_key_;
  int num_probes_;
  std::vector<uint64_t> hash_entries_;
};

// See description in FastLocalBloomImpl
class FastLocalBloomBitsReader : public FilterBitsReader {
 public:
  FastLocalBloomBitsReader(const char* data, int num_probes, uint32_t len_bytes)
      : data_(data), num_probes_(num_probes), len_bytes_(len_bytes) {}

  // No Copy allowed
  FastLocalBloomBitsReader(const FastLocalBloomBitsReader&) = delete;
  void operator=(const FastLocalBloomBitsReader&) = delete;

  ~FastLocalBloomBitsReader() override {}

  bool MayMatch(const Slice& key) override {
    uint64_t h = GetSliceHash64(key);
    uint32_t byte_offset;
    FastLocalBloomImpl::PrepareHash(Lower32of64(h), len_bytes_, data_,
                                    /*out*/ &byte_offset);
    return FastLocalBloomImpl::HashMayMatchPrepared(Upper32of64(h), num_probes_,
                                                    data_ + byte_offset);
  }

  virtual void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> hashes;
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> byte_offsets;
    for (int i = 0; i < num_keys; ++i) {
      uint64_t h = GetSliceHash64(*keys[i]);
      FastLocalBloomImpl::PrepareHash(Lower32of64(h), len_bytes_, data_,
                                      /*out*/ &byte_offsets[i]);
      hashes[i] = Upper32of64(h);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = FastLocalBloomImpl::HashMayMatchPrepared(
          hashes[i], num_probes_, data_ + byte_offsets[i]);
    }
  }

 private:
  const char* data_;
  const int num_probes_;
  const uint32_t len_bytes_;
};

using LegacyBloomImpl = LegacyLocalityBloomImpl</*ExtraRotates*/ false>;

class LegacyBloomBitsBuilder : public BuiltinFilterBitsBuilder {
 public:
  explicit LegacyBloomBitsBuilder(const int bits_per_key);

  // No Copy allowed
  LegacyBloomBitsBuilder(const LegacyBloomBitsBuilder&) = delete;
  void operator=(const LegacyBloomBitsBuilder&) = delete;

  ~LegacyBloomBitsBuilder() override;

  void AddKey(const Slice& key) override;

  Slice Finish(std::unique_ptr<const char[]>* buf) override;

  int CalculateNumEntry(const uint32_t bytes) override;

  uint32_t CalculateSpace(const int num_entry) override {
    uint32_t dont_care1;
    uint32_t dont_care2;
    return CalculateSpace(num_entry, &dont_care1, &dont_care2);
  }

 private:
  int bits_per_key_;
  int num_probes_;
  std::vector<uint32_t> hash_entries_;

  // Get totalbits that optimized for cpu cache line
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);

  // Reserve space for new filter
  char* ReserveSpace(const int num_entry, uint32_t* total_bits,
                     uint32_t* num_lines);

  // Implementation-specific variant of public CalculateSpace
  uint32_t CalculateSpace(const int num_entry, uint32_t* total_bits,
                          uint32_t* num_lines);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);
};

LegacyBloomBitsBuilder::LegacyBloomBitsBuilder(const int bits_per_key)
    : bits_per_key_(bits_per_key),
      num_probes_(LegacyNoLocalityBloomImpl::ChooseNumProbes(bits_per_key_)) {
  assert(bits_per_key_);
}

LegacyBloomBitsBuilder::~LegacyBloomBitsBuilder() {}

void LegacyBloomBitsBuilder::AddKey(const Slice& key) {
  uint32_t hash = BloomHash(key);
  if (hash_entries_.size() == 0 || hash != hash_entries_.back()) {
    hash_entries_.push_back(hash);
  }
}

Slice LegacyBloomBitsBuilder::Finish(std::unique_ptr<const char[]>* buf) {
  uint32_t total_bits, num_lines;
  char* data = ReserveSpace(static_cast<int>(hash_entries_.size()), &total_bits,
                            &num_lines);
  assert(data);

  if (total_bits != 0 && num_lines != 0) {
    for (auto h : hash_entries_) {
      AddHash(h, data, num_lines, total_bits);
    }
  }
  // See BloomFilterPolicy::GetFilterBitsReader for metadata
  data[total_bits / 8] = static_cast<char>(num_probes_);
  EncodeFixed32(data + total_bits / 8 + 1, static_cast<uint32_t>(num_lines));

  const char* const_data = data;
  buf->reset(const_data);
  hash_entries_.clear();

  return Slice(data, total_bits / 8 + 5);
}

uint32_t LegacyBloomBitsBuilder::GetTotalBitsForLocality(uint32_t total_bits) {
  uint32_t num_lines =
      (total_bits + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);

  // Make num_lines an odd number to make sure more bits are involved
  // when determining which block.
  if (num_lines % 2 == 0) {
    num_lines++;
  }
  return num_lines * (CACHE_LINE_SIZE * 8);
}

uint32_t LegacyBloomBitsBuilder::CalculateSpace(const int num_entry,
                                                uint32_t* total_bits,
                                                uint32_t* num_lines) {
  assert(bits_per_key_);
  if (num_entry != 0) {
    uint32_t total_bits_tmp = static_cast<uint32_t>(num_entry * bits_per_key_);

    *total_bits = GetTotalBitsForLocality(total_bits_tmp);
    *num_lines = *total_bits / (CACHE_LINE_SIZE * 8);
    assert(*total_bits > 0 && *total_bits % 8 == 0);
  } else {
    // filter is empty, just leave space for metadata
    *total_bits = 0;
    *num_lines = 0;
  }

  // Reserve space for Filter
  uint32_t sz = *total_bits / 8;
  sz += 5;  // 4 bytes for num_lines, 1 byte for num_probes
  return sz;
}

char* LegacyBloomBitsBuilder::ReserveSpace(const int num_entry,
                                           uint32_t* total_bits,
                                           uint32_t* num_lines) {
  uint32_t sz = CalculateSpace(num_entry, total_bits, num_lines);
  char* data = new char[sz];
  memset(data, 0, sz);
  return data;
}

int LegacyBloomBitsBuilder::CalculateNumEntry(const uint32_t bytes) {
  assert(bits_per_key_);
  assert(bytes > 0);
  int high = static_cast<int>(bytes * 8 / bits_per_key_ + 1);
  int low = 1;
  int n = high;
  for (; n >= low; n--) {
    if (CalculateSpace(n) <= bytes) {
      break;
    }
  }
  assert(n < high);  // High should be an overestimation
  return n;
}

inline void LegacyBloomBitsBuilder::AddHash(uint32_t h, char* data,
                                            uint32_t num_lines,
                                            uint32_t total_bits) {
#ifdef NDEBUG
  static_cast<void>(total_bits);
#endif
  assert(num_lines > 0 && total_bits > 0);

  LegacyBloomImpl::AddHash(h, num_lines, num_probes_, data,
                           folly::constexpr_log2(CACHE_LINE_SIZE));
}

class LegacyBloomBitsReader : public FilterBitsReader {
 public:
  LegacyBloomBitsReader(const char* data, int num_probes, uint32_t num_lines,
                        uint32_t log2_cache_line_size)
      : data_(data),
        num_probes_(num_probes),
        num_lines_(num_lines),
        log2_cache_line_size_(log2_cache_line_size) {}

  // No Copy allowed
  LegacyBloomBitsReader(const LegacyBloomBitsReader&) = delete;
  void operator=(const LegacyBloomBitsReader&) = delete;

  ~LegacyBloomBitsReader() override {}

  // "contents" contains the data built by a preceding call to
  // FilterBitsBuilder::Finish. MayMatch must return true if the key was
  // passed to FilterBitsBuilder::AddKey. This method may return true or false
  // if the key was not on the list, but it should aim to return false with a
  // high probability.
  bool MayMatch(const Slice& key) override {
    uint32_t hash = BloomHash(key);
    uint32_t byte_offset;
    LegacyBloomImpl::PrepareHashMayMatch(
        hash, num_lines_, data_, /*out*/ &byte_offset, log2_cache_line_size_);
    return LegacyBloomImpl::HashMayMatchPrepared(
        hash, num_probes_, data_ + byte_offset, log2_cache_line_size_);
  }

  virtual void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> hashes;
    std::array<uint32_t, MultiGetContext::MAX_BATCH_SIZE> byte_offsets;
    for (int i = 0; i < num_keys; ++i) {
      hashes[i] = BloomHash(*keys[i]);
      LegacyBloomImpl::PrepareHashMayMatch(hashes[i], num_lines_, data_,
                                           /*out*/ &byte_offsets[i],
                                           log2_cache_line_size_);
    }
    for (int i = 0; i < num_keys; ++i) {
      may_match[i] = LegacyBloomImpl::HashMayMatchPrepared(
          hashes[i], num_probes_, data_ + byte_offsets[i],
          log2_cache_line_size_);
    }
  }

 private:
  const char* data_;
  const int num_probes_;
  const uint32_t num_lines_;
  const uint32_t log2_cache_line_size_;
};

class AlwaysTrueFilter : public FilterBitsReader {
 public:
  bool MayMatch(const Slice&) override { return true; }
  using FilterBitsReader::MayMatch;  // inherit overload
};

class AlwaysFalseFilter : public FilterBitsReader {
 public:
  bool MayMatch(const Slice&) override { return false; }
  using FilterBitsReader::MayMatch;  // inherit overload
};

}  // namespace

const std::vector<BloomFilterPolicy::Mode> BloomFilterPolicy::kAllFixedImpls = {
    kLegacyBloom,
    kDeprecatedBlock,
    kFastLocalBloom,
    kOtLexPdt,
};

const std::vector<BloomFilterPolicy::Mode> BloomFilterPolicy::kAllUserModes = {
    kDeprecatedBlock,
    kAuto,
};

BloomFilterPolicy::BloomFilterPolicy(double bits_per_key, Mode mode)
    : mode_(mode) {
  // Sanitize bits_per_key
  if (bits_per_key < 1.0) {
    bits_per_key = 1.0;
  } else if (!(bits_per_key < 100.0)) {  // including NaN
    bits_per_key = 100.0;
  }

  // Includes a nudge toward rounding up, to ensure on all platforms
  // that doubles specified with three decimal digits after the decimal
  // point are interpreted accurately.
  millibits_per_key_ = static_cast<int>(bits_per_key * 1000.0 + 0.500001);

  // For better or worse, this is a rounding up of a nudged rounding up,
  // e.g. 7.4999999999999 will round up to 8, but that provides more
  // predictability against small arithmetic errors in floating point.
  whole_bits_per_key_ = (millibits_per_key_ + 500) / 1000;
}

BloomFilterPolicy::~BloomFilterPolicy() {}

const char* BloomFilterPolicy::Name() const {
  if (mode_ == kOtLexPdt) {
    return "rocksdb.OtLexPdtFilter";
  }
  return "rocksdb.BuiltinBloomFilter";
}

void BloomFilterPolicy::CreateFilter(const Slice* keys, int n,
                                     std::string* dst) const {
  // We should ideally only be using this deprecated interface for
  // appropriately constructed BloomFilterPolicy
  // FIXME disabled because of bug in C interface; see issue #6129
  //assert(mode_ == kDeprecatedBlock);

  // Compute bloom filter size (in both bits and bytes)
  uint32_t bits = static_cast<uint32_t>(n * whole_bits_per_key_);

  // For small n, we can see a very high false positive rate.  Fix it
  // by enforcing a minimum bloom filter length.
  if (bits < 64) bits = 64;

  uint32_t bytes = (bits + 7) / 8;
  bits = bytes * 8;

  int num_probes =
      LegacyNoLocalityBloomImpl::ChooseNumProbes(whole_bits_per_key_);

  const size_t init_size = dst->size();
  dst->resize(init_size + bytes, 0);
  dst->push_back(static_cast<char>(num_probes));  // Remember # of probes
  char* array = &(*dst)[init_size];
  for (int i = 0; i < n; i++) {
    LegacyNoLocalityBloomImpl::AddHash(BloomHash(keys[i]), bits, num_probes,
                                       array);
  }
}

bool BloomFilterPolicy::KeyMayMatch(const Slice& key,
                                    const Slice& bloom_filter) const {
  const size_t len = bloom_filter.size();
  if (len < 2 || len > 0xffffffffU) {
    return false;
  }

  const char* array = bloom_filter.data();
  const uint32_t bits = static_cast<uint32_t>(len - 1) * 8;

  // Use the encoded k so that we can read filters generated by
  // bloom filters created using different parameters.
  const int k = static_cast<uint8_t>(array[len - 1]);
  if (k > 30) {
    // Reserved for potentially new encodings for short bloom filters.
    // Consider it a match.
    return true;
  }
  // NB: using stored k not num_probes for whole_bits_per_key_
  return LegacyNoLocalityBloomImpl::HashMayMatch(BloomHash(key), bits, k,
                                                 array);
}

FilterBitsBuilder* BloomFilterPolicy::GetFilterBitsBuilder() const {
  // This code path should no longer be used, for the built-in
  // BloomFilterPolicy. Internal to RocksDB and outside
  // BloomFilterPolicy, only get a FilterBitsBuilder with
  // BloomFilterPolicy::GetBuilderFromContext(), which will call
  // BloomFilterPolicy::GetBuilderWithContext(). RocksDB users have
  // been warned (HISTORY.md) that they can no longer call this on
  // the built-in BloomFilterPolicy (unlikely).
  assert(false);
  return GetBuilderWithContext(FilterBuildingContext(BlockBasedTableOptions()));
}

FilterBitsBuilder* BloomFilterPolicy::GetBuilderWithContext(
    const FilterBuildingContext& context) const {
  Mode cur = mode_;
  // Unusual code construction so that we can have just
  // one exhaustive switch without (risky) recursion
  for (int i = 0; i < 2; ++i) {
    switch (cur) {
      case kAuto:
        if (context.table_options.format_version < 5) {
          cur = kLegacyBloom;
        } else {
          if (context.table_options.use_pdt) { //xp
            cur = kOtLexPdt;
          }
          else {
            cur = kFastLocalBloom;
          }
        }
        //        fprintf(stdout, "in GetBuilderWithContext(), mode: %d,
        //        format_version: %d\n",
        //                cur, context.table_options.format_version); //xp
        break;
      case kDeprecatedBlock:
        return nullptr;
      case kFastLocalBloom:
        //fprintf(stderr, "new FastLocalBloomBitsBuilder()\n");
        return new FastLocalBloomBitsBuilder(millibits_per_key_);
      case kLegacyBloom:
        //fprintf(stderr, "new LegacyBloomBitsBuilder()\n");
        return new LegacyBloomBitsBuilder(whole_bits_per_key_);
      case kOtLexPdt:  // xp
        //fprintf(stderr, "new OtLexPdtBitsBuilder()\n");
        return new OtLexPdtBloomBitsBuilder();
    }
  }
  assert(false);
  return nullptr;  // something legal
}

FilterBitsBuilder* BloomFilterPolicy::GetBuilderFromContext(
    const FilterBuildingContext& context) {
  if (context.table_options.filter_policy) {
    return context.table_options.filter_policy->GetBuilderWithContext(context);
  } else {
    return nullptr;
  }
}

// Read metadata to determine what kind of FilterBitsReader is needed
// and return a new one.
FilterBitsReader* BloomFilterPolicy::GetFilterBitsReader(
    const Slice& contents) const {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  if (len_with_meta <= 5) {
    // filter is empty or broken. Treat like zero keys added.
    return new AlwaysFalseFilter();
  }

  // Legacy Bloom filter data:
  //             0 +-----------------------------------+
  //               | Raw Bloom filter data             |
  //               | ...                               |
  //           len +-----------------------------------+
  //               | byte for num_probes or            |
  //               |   marker for new implementations  |
  //         len+1 +-----------------------------------+
  //               | four bytes for number of cache    |
  //               |   lines                           |
  // len_with_meta +-----------------------------------+

  int8_t raw_num_probes =
      static_cast<int8_t>(contents.data()[len_with_meta - 5]);
  // NB: *num_probes > 30 and < 128 probably have not been used, because of
  // BloomFilterPolicy::initialize, unless directly calling
  // LegacyBloomBitsBuilder as an API, but we are leaving those cases in
  // limbo with LegacyBloomBitsReader for now.

  if (raw_num_probes < 1) {
    // Note: < 0 (or unsigned > 127) indicate special new implementations
    // (or reserved for future use)
    if (raw_num_probes == -1) {
      // Marker for newer Bloom implementations
      return GetBloomBitsReader(contents);
    }
    // otherwise
    // Treat as zero probes (always FP) for now.
    return new AlwaysTrueFilter();
  }
  // else attempt decode for LegacyBloomBitsReader

  int num_probes = raw_num_probes;
  assert(num_probes >= 1);
  assert(num_probes <= 127);

  uint32_t len = len_with_meta - 5;
  assert(len > 0);

  uint32_t num_lines = DecodeFixed32(contents.data() + len_with_meta - 4);
  uint32_t log2_cache_line_size;

  if (num_lines * CACHE_LINE_SIZE == len) {
    // Common case
    log2_cache_line_size = folly::constexpr_log2(CACHE_LINE_SIZE);
  } else if (num_lines == 0 || len % num_lines != 0) {
    // Invalid (no solution to num_lines * x == len)
    // Treat as zero probes (always FP) for now.
    return new AlwaysTrueFilter();
  } else {
    // Determine the non-native cache line size (from another system)
    log2_cache_line_size = 0;
    while ((num_lines << log2_cache_line_size) < len) {
      ++log2_cache_line_size;
    }
    if ((num_lines << log2_cache_line_size) != len) {
      // Invalid (block size not a power of two)
      // Treat as zero probes (always FP) for now.
      return new AlwaysTrueFilter();
    }
  }
  // if not early return
  return new LegacyBloomBitsReader(contents.data(), num_probes, num_lines,
                                   log2_cache_line_size);
}

// For newer Bloom filter implementations
FilterBitsReader* BloomFilterPolicy::GetBloomBitsReader(
    const Slice& contents) const {
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  uint32_t len = len_with_meta - 5;

  assert(len > 0);  // precondition

  // New Bloom filter data:
  //             0 +-----------------------------------+
  //               | Raw Bloom filter data             |
  //               | ...                               |
  //           len +-----------------------------------+
  //               | char{-1} byte -> new Bloom filter |
  //         len+1 +-----------------------------------+
  //               | byte for subimplementation        |
  //               |   0: FastLocalBloom               |
  //               |   other: reserved                 |
  //         len+2 +-----------------------------------+
  //               | byte for block_and_probes         |
  //               |   0 in top 3 bits -> 6 -> 64-byte |
  //               |   reserved:                       |
  //               |   1 in top 3 bits -> 7 -> 128-byte|
  //               |   2 in top 3 bits -> 8 -> 256-byte|
  //               |   ...                             |
  //               |   num_probes in bottom 5 bits,    |
  //               |     except 0 and 31 reserved      |
  //         len+3 +-----------------------------------+
  //               | two bytes reserved                |
  //               |   possibly for hash seed          |
  // len_with_meta +-----------------------------------+

  // Read more metadata (see above)
  char sub_impl_val = contents.data()[len_with_meta - 4];
  char block_and_probes = contents.data()[len_with_meta - 3];
  int log2_block_bytes = ((block_and_probes >> 5) & 7) + 6;

  int num_probes = (block_and_probes & 31);
  if (num_probes < 1 || num_probes > 30) {
    // Reserved / future safe
    return new AlwaysTrueFilter();
  }

  uint16_t rest = DecodeFixed16(contents.data() + len_with_meta - 2);
  if (rest != 0) {
    // Reserved, possibly for hash seed
    // Future safe
    return new AlwaysTrueFilter();
  }

  if(80 == sub_impl_val) {
//    if(mode_ != kOtLexPdt) {
//      fprintf(stderr, "WARNING &7vzh GetBloomBitsReader sub_impl_val 5 but mode_ %d NOT kOT\n", mode_);
//    }
    //fprintf(stderr, "DEBUG a8uq9 GetBloomBitsReader use Ot, mode_ %d\n", mode_);
    return new OtLexPdtBloomBitsReader(contents.data());
  }

  if (sub_impl_val == 0) {        // FastLocalBloom
    if (log2_block_bytes == 6) {  // Only block size supported for now
      return new FastLocalBloomBitsReader(contents.data(), num_probes, len);
    }
  }
  // otherwise
  // Reserved / future safe
  return new AlwaysTrueFilter();
}

const FilterPolicy* NewBloomFilterPolicy(double bits_per_key,
                                         bool use_block_based_builder) {
  BloomFilterPolicy::Mode m;
  if(-1 == bits_per_key) { //xp
    m = BloomFilterPolicy::kOtLexPdt;
    return new BloomFilterPolicy(0, m);
  }
  if (use_block_based_builder) {
    m = BloomFilterPolicy::kDeprecatedBlock;
  } else {
    m = BloomFilterPolicy::kAuto;
  }
  //  fprintf(stdout, "filter mode: %d\n", m);//xp
  assert(std::find(BloomFilterPolicy::kAllUserModes.begin(),
                   BloomFilterPolicy::kAllUserModes.end(),
                   m) != BloomFilterPolicy::kAllUserModes.end());
  return new BloomFilterPolicy(bits_per_key, m);
}

// xp
// actively init Mode with kOtLexPdt
const FilterPolicy* NewOtLexPdtFilterPolicy() {
  BloomFilterPolicy::Mode m;
  m = BloomFilterPolicy::kOtLexPdt;
  //  if (use_block_based_builder) {
  //    m = BloomFilterPolicy::kDeprecatedBlock;
  //  } else {
  //    m = BloomFilterPolicy::kAuto;
  //  }
  return new BloomFilterPolicy(0, m);
}

FilterBuildingContext::FilterBuildingContext(
    const BlockBasedTableOptions& _table_options)
    : table_options(_table_options) {}

FilterPolicy::~FilterPolicy() { }

}  // namespace rocksdb
