#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <map>
#include <unistd.h>
#include <vector>
#include <memcached-imitation/connection_io_server.h>
#include <memcached-imitation/lock_token.h>
#include <memcached-imitation/connection.h>

namespace NessieCache {
    
void ConnectionIOServer::remove_connection(int fd, fd_set* rfds, fd_set* wfds) {
    FD_CLR(fd, rfds);
    FD_CLR(fd, wfds);
    std::map<int, Connection*>::iterator it = connection_map_.find(fd); 
    assert(it != connection_map_.end());
    // Delete the connection object.
    delete it->second;
    // Remove from the connection map
    connection_map_.erase(it);
}
    
int ConnectionIOServer::add_connection(fd_set* rfds) {
    LockToken token(&lock_);
    // Drain the entire pipe buffer                
    int pipe_rc = 0;
    while (pipe_rc != -1) {
        pipe_rc = read(pipefd_[0], drain_buffer_, DRAIN_BUF_SIZE);
    }
    for (unsigned i = 0; i < new_sockets_.size(); ++i) {
        int cur_sock = new_sockets_[i];
        if (cur_sock == -1) {
            return -1; // Termination message    
        }        
        FD_SET(cur_sock, rfds);
        connection_map_[cur_sock] = new Connection(cur_sock);                    
    }
    new_sockets_.clear();
    return 0;
}        

void ConnectionIOServer::InternalThreadEntry() {
    fd_set master_rfds, working_rfds;
    fd_set master_wfds, working_wfds;
    FD_ZERO(&master_rfds);
    FD_ZERO(&master_wfds);
    FD_SET(pipefd_[0], &master_rfds);       
    
    // Keep track of closed connections
    std::vector<int> remove_list;
    
    while (1) {
        working_rfds = master_rfds;
        working_wfds = master_wfds;
                    
        // Determine the maximum file descriptor.
        int max_fd = connection_map_.empty() ? pipefd_[0] : 
            std::max(pipefd_[0], (connection_map_.rbegin())->first);
        
        // Block until there is activity
        int activity = select(max_fd + 1, &working_rfds, &working_wfds, 
                              NULL, NULL);
        
        // Check to see if there was an error with select.
        if (activity < 0 && errno != EINTR) {
            return; // TODO: Improve cleanup on error. 
        }
        
        // See if there are new items to sockets to add
        if (FD_ISSET(pipefd_[0], &working_rfds)) {
            if (add_connection(&master_rfds) == -1) {
                return; // TODO: Should clean up connections                
            }
        }
        
        // See if any of the connections is ready for read/write
        for (std::map<int, Connection*>::iterator it = connection_map_.begin();
                it != connection_map_.end(); it++) {
            if (FD_ISSET(it->first, &working_wfds)) {
                int w_result = it->second->flush_writes();
                if (w_result == -1) {
                    remove_list.push_back(it->first);
                    continue; // Done with this socket
                } else if (w_result == 0) {
                    // Completely finished flushing the writes for this
                    // socket. Remove it from the fd_set
                    FD_CLR(it->first, &master_wfds);                        
                }
            }            
            if (FD_ISSET(it->first, &working_rfds)) {
                int r_result = it->second->handle_request();
                if (r_result == -1) {
                    remove_list.push_back(it->first);
                    continue; // Done with this socket                        
                }
                // Check if this socket needs to be added to the wfds.
                if (it->second->pending_writes_exist()) {
                    FD_SET(it->first, &master_wfds);    
                }                    
            }
        }
        
        // Check if there are any connections that need to be removed.
        if (!remove_list.empty()) {
            for (unsigned i = 0; i < remove_list.size(); ++i) {
                remove_connection(remove_list[i], &master_rfds, &master_wfds);
            }
            remove_list.clear();
        }
    }        
}
    
ConnectionIOServer::ConnectionIOServer() : null_byte_('\0') {
    pipefd_[0] = -1;
    pipefd_[1] = -1;
    pthread_mutex_init(&lock_, NULL);
}
    
ConnectionIOServer::~ConnectionIOServer() {
    if (pipefd_[0] != -1 && pipefd_[1] != -1) {
        add_socket_to_IO_server(-1);
        WaitForInternalThreadToExit();
        close(pipefd_[0]); 
        close(pipefd_[1]);
    }
    pthread_mutex_destroy(&lock_);
}
    
// TODO: Unbounded queue. Should change this to a bounded queue.
void ConnectionIOServer::add_socket_to_IO_server(int sock) {
    LockToken token(&lock_);
    new_sockets_.push_back(sock);
    if (new_sockets_.size() == 1) {
        write(pipefd_[1], &null_byte_, 1);
    }
}
    
int ConnectionIOServer::start() {
    if (pipe2(pipefd_, O_NONBLOCK) == -1) {          
        return -1;    
    }        
    return StartInternalThread();        
}
}


