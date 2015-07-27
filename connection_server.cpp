#include <arpa/inet.h>    //close
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <memcached-imitation/connection_server.h>
#include <memcached-imitation/connection_io_server.h>
#include <memcached-imitation/socket_util.h>

using namespace std;

namespace NessieCache {   
    
void ConnectionServer::add_socket_to_IO_server(int sock) {
    io_servers_[io_server_counter_++]->add_socket_to_IO_server(sock);
    if (io_server_counter_ >= io_servers_.size()) {
        io_server_counter_ = 0;
    }
}
    
int ConnectionServer::create_server_socket(int port) {
    int sock;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        return -1;
    }
    
    if (SocketUtil::set_non_block(sock) == -1) {
        return -1;            
    }            
    
    // NOTE: This should be updated to correctly support IP V6.
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    if (bind(sock, (struct sockaddr*)(&address), sizeof(address)) == -1) {
        return -1;    
    }    
    if (listen(sock, back_log_size_) == -1) {
        return -1;    
    }
    return sock;
}   
  
ConnectionServer::ConnectionServer(int port, int num_io_servers) 
    : control_socket_(-1), server_socket_(-1), port_(port), 
      io_server_counter_(0) {
    for (int i = 0; i < num_io_servers; ++i) {
        ConnectionIOServer* new_server = new ConnectionIOServer();        
        //assert(new_server != NULL);
        if (new_server == NULL || new_server->start() == -1) {
            abort(); // Just abort for now instead of handling this case. 
        }
        io_servers_.push_back(new_server);
    }
}
    
int ConnectionServer::start() {
    // 1. Create a non-blocking server socket.
    // 2. Call select on the socket
    // 3. Upon unblocking from select, call accept in a loop until
    //    it returns a EWOULDBLOCK.
    
    // Create control port. This is used for process termination.
    if ((control_socket_ = create_server_socket(control_port_)) == -1) {
        return -1;        
    }        
    // Create data port
    if ((server_socket_ = create_server_socket(port_)) == -1) {
        return -1;        
    }
    
    fd_set master_fds, working_fds;
    FD_ZERO(&master_fds);
    FD_SET(control_socket_, &master_fds);
    FD_SET(server_socket_, &master_fds);        
    
    while (1) {
        working_fds = master_fds;
        int activity = select(max(control_socket_, server_socket_) + 1, 
                              &working_fds, NULL, NULL, NULL);            
        if (activity < 0 && errno != EINTR) {
            return -1;                
        }
        if (FD_ISSET(control_socket_, &working_fds)) {
            // For now, termine if we receive a connection request. In the
            // future, we should require a "CLOSE" message.
            return -1;
        }            
        if (FD_ISSET(server_socket_, &working_fds)) {       
            // For now, we don't care what the client address is
            while (1) {
                int new_sock = accept(server_socket_, NULL, NULL);
                if (new_sock == -1) {
                    if (errno == EWOULDBLOCK) {                            
                        break; // No more connections to accept
                    } else {
                        return -1;
                    }                    
                } else {
                    if (SocketUtil::set_non_block(new_sock) == -1) {
                        printf("asdf]=\n");
                        return -1;            
                    }
                    SocketUtil::turn_off_nagle(new_sock);
                    add_socket_to_IO_server(new_sock);                            
                }
            }
        }
    }            
}
    
ConnectionServer::~ConnectionServer() {
    if (control_socket_ != -1) {
        close(control_socket_);
    }                
    if (server_socket_ != -1) {
        close(server_socket_);            
    }        
    printf("Deleting IO servers\n");
    for (unsigned i = 0; i < io_servers_.size(); ++i) {
        delete io_servers_[i];    
    }
    printf("Done\n");
}    
} // Namespace NessieCache

using namespace NessieCache;

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s port num_io_servers\n", argv[0]);
        return -1;
    }
    ConnectionServer cs(atoi(argv[1]), atoi(argv[2]));
    cs.start();  
    return 0;
}
