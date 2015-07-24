#ifndef CONNECTION_IO_SERVER_H
#define CONNECTION_IO_SERVER_H

#define DRAIN_BUF_SIZE 4096

#include <pthread.h>
#include <vector>
#include <map>
#include <sys/select.h>
#include "my_thread_class.h"

namespace NessieCache {
    
class Connection;
    
class ConnectionIOServer : public MyThreadClass {
  private:
    int pipefd_[2];
    const char null_byte_;
    pthread_mutex_t lock_;
    std::vector<int> new_sockets_;
    std::map<int, Connection*> connection_map_;
    char drain_buffer_[DRAIN_BUF_SIZE];
        
    void remove_connection(int fd, fd_set* rfds, fd_set* wfds);    
    int add_connection(fd_set* rfds);

  protected:
    virtual void InternalThreadEntry();
    
  public:
    ConnectionIOServer();    
    virtual ~ConnectionIOServer();    
    void add_socket_to_IO_server(int sock);    
    int start();
};
}
#endif

