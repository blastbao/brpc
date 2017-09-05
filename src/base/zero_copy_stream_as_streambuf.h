// Copyright (c) 2012 baidu-rpc authors.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Author: Ge,Jun (gejun@baidu.com)
// Date: Thu Nov 22 13:57:56 CST 2012

#ifndef BAIDU_BASE_ZERO_COPY_STREAM_AS_STREAMBUF_H
#define BAIDU_BASE_ZERO_COPY_STREAM_AS_STREAMBUF_H

#include <streambuf>
#include <google/protobuf/io/zero_copy_stream.h>

namespace base {

// Wrap a ZeroCopyOutputStream into std::streambuf. Notice that before 
// destruction or shrink(), BackUp() of the stream are not called. In another
// word, if the stream is wrapped from IOBuf, the IOBuf may be larger than 
// appended data.
class ZeroCopyStreamAsStreamBuf : public std::streambuf {
public:
    ZeroCopyStreamAsStreamBuf(google::protobuf::io::ZeroCopyOutputStream* stream)
        : _zero_copy_stream(stream) {}
    virtual ~ZeroCopyStreamAsStreamBuf();

    // BackUp() unused bytes. Automatically called in destructor.
    void shrink();
    
protected:
    virtual int overflow(int ch);
    virtual int sync();
    std::streampos seekoff(std::streamoff off,
                           std::ios_base::seekdir way,
                           std::ios_base::openmode which);

private:
    google::protobuf::io::ZeroCopyOutputStream* _zero_copy_stream;
};

}  // namespace base

#endif  // BAIDU_BASE_ZERO_COPY_STREAM_AS_STREAMBUF_H