#ifndef CONNECTION_H
#define CONNECTION_H

#include <list>
#include <stdint.h>
#include <string>

namespace NessieCache {

#define BUFFER_LENGTH (64 * 1024)
    
struct RequestHeader {
    uint8_t magic;
    uint8_t opcode;
    uint16_t keylength;
    uint8_t extralength;
    uint8_t datatype;
    uint16_t vbucket_id;
    uint32_t totalbody;
    uint32_t opaque;
    uint64_t cas; // Unused
}__attribute__((packed));

struct Request {
    struct RequestHeader header;
    char* body;
};
    
class Connection {
  private:
    int sock_;
    char buf_[BUFFER_LENGTH];
    int buf_used_;
    std::list<std::string> out_bufs_;
    
    int fill_buffer();    
    int process_next_request();    
    int fetch_request(struct Request* req);    
    void copy_to_write_buf(int offset, char* src, int src_len);    
    int sendToNessieFake(int magic, int opcode, int keylength, int totalbody, 
                         int extralength, char* opaque, char* buffer);
    
  public:
    Connection(int sock) : sock_(sock), buf_used_(0) {}
    virtual ~Connection() {}
    
    int flush_writes();    
    bool pending_writes_exist();        
    int handle_request();
};
}
#endif