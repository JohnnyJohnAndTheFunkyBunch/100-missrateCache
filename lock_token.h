#ifndef LOCK_TOKEN_H
#define LOCK_TOKEN_H

#include <pthread.h>

namespace NessieCache {

class LockToken {
  private:
    pthread_mutex_t* lock;
    bool valid;

  public:
    LockToken(pthread_mutex_t* lock) : lock(lock), valid(true) {
        if (pthread_mutex_lock(lock) != 0) {
            valid = false;            
        }
    }    
    ~LockToken() { if (valid) pthread_mutex_unlock(lock); }
    bool is_valid() const { return valid; }
    void release_lock() {
        if (valid) {
            pthread_mutex_unlock(lock);
            valid = false;
        }
    }
};
}
#endif