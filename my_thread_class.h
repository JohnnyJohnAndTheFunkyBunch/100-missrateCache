#ifndef MY_THREAD_CLASS_H
#define MY_THREAD_CLASS_H

#include <pthread.h>

namespace NessieCache {

// Taken from stackoverflow:
// http://stackoverflow.com/questions/1151582/pthread-function-from-a-class
class MyThreadClass {
  public:
    MyThreadClass() {/* empty */}
    virtual ~MyThreadClass() {/* empty */}

    /** Returns true if the thread was successfully started, false if there was
        an error starting the thread */
    int StartInternalThread() {
        return pthread_create(&_thread, NULL, InternalThreadEntryFunc, this);
    }

    /** Will not return until the internal thread has exited. */
    void WaitForInternalThreadToExit() {
        (void) pthread_join(_thread, NULL);
    }

  protected:
    /** Implement this method in your subclass with the code you want your 
        thread to run. */
    virtual void InternalThreadEntry() = 0;

  private:
    static void * InternalThreadEntryFunc(void * This) {
        ((MyThreadClass *)This)->InternalThreadEntry(); return NULL;}
    pthread_t _thread;
};
}
#endif