#ifndef CONNECTION_SERVER_H
#define CONNECTION_SERVER_H

#include <vector>

namespace NessieCache {
    
class ConnectionIOServer;

class ConnectionServer {
  private:
    int control_socket_;
    int server_socket_;    
    int port_;     
    unsigned io_server_counter_;
    const static int control_port_ = 5234; // Hardcode the control port for now.
    const static int back_log_size_ = 100;
    std::vector<ConnectionIOServer*> io_servers_;
    
    void add_socket_to_IO_server(int sock);    
    static int create_server_socket(int port);
     
  public:
    ConnectionServer(int port, int num_io_servers);
    ~ConnectionServer();    
    int start();
};
} // Namespace NessieCache
#endif