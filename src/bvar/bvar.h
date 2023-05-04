// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Date: 2014/12/29 14:54:11

#ifndef  BVAR_BVAR_H
#define  BVAR_BVAR_H

#include "bvar/reducer.h"
#include "bvar/recorder.h"
#include "bvar/status.h"
#include "bvar/passive_status.h"
#include "bvar/latency_recorder.h"
#include "bvar/gflag.h"
#include "bvar/scoped_timer.h"
#include "bvar/mvariable.h"

#endif  //BVAR_BVAR_H


// bvar 最核心的思想就是利用 thread local 变量来减少 cache bouncing ，本质上是将写的竞争转移到了读，
// 但在诸如监控这种场景下，通常读是远远小于写的，因此这种转移的正向效果是显著的。