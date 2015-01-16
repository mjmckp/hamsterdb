/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * The worker thread. Asynchronously purges the cache. Thread will start as
 * soon as it's constructed.
 */

#ifndef HAM_WORKER_H
#define HAM_WORKER_H

#include "0root/root.h"

#include <boost/thread.hpp>

// Always verify that a file of level N does not include headers > N!
#include "2queue/queue.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Worker
{
  public:
    Worker(LocalEnvironment *env)
      : m_env(env), m_stop_requested(false), m_thread(&Worker::run, this) {
    }

    void add_to_queue(MessageBase *message) {
      m_queue.push(message);

      ScopedLock lock(m_mutex);
      m_cond.notify_one();
    }

    void stop_and_join() {
      {
        ScopedLock lock(m_mutex);
        m_stop_requested = true;
        m_cond.notify_one();
      }
      m_thread.join();
    }

  private:
    // The thread function
    void run() {
      ScopedLock lock(m_mutex);
      while (!m_stop_requested) {
        m_cond.wait(lock); // will unlock m_mutex while waiting

        MessageBase *message;
        while ((message = m_queue.pop()) != 0) {
          // ignore the message if stop was requested and the message is not
          // mandatory
          if ((m_stop_requested && message->flags & MessageBase::kIsMandatory)
                  || !m_stop_requested) {
            // handle the message
          }
          delete message;
        }
        //m_env->get_page_manager()->purge_cache();
        //boost::this_thread::yield();
      }
    }

    // The Environment which created the thread
    LocalEnvironment *m_env;

    // A queue for storing messages
    Queue m_queue;

    // true if the Environment is closed
    bool m_stop_requested;

    // A mutex for protecting |m_cond|
    boost::mutex m_mutex;

    // A condition to wait for
    boost::condition_variable m_cond;

    // The actual thread
    boost::thread m_thread;
};

} // namespace hamsterdb

#endif // HAM_WORKER_H
