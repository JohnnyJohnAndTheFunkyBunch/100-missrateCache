#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <memcached-imitation/connection.h>

namespace NessieCache {

int Connection::fill_buffer() {
    while (buf_used_ < BUFFER_LENGTH) {
        int recv_len = recv(sock_, buf_ + buf_used_,
                            BUFFER_LENGTH - buf_used_, 0);
        if (recv_len == -1) {
            return (errno == EWOULDBLOCK || errno == EAGAIN) ? 0 : -1;
        } else if (recv_len == 0) {
            return -1;    // Closed
        } 
        buf_used_ += recv_len;
    }
    return 1; // Buffer full but there more to read.
}
    
int Connection::process_next_request(int* offset) {
    struct Request req;
    struct RequestHeader* header = &(req.header);
    int amount_read;
    int fetch_rc = fetch_request(&req, *offset, &amount_read);
    if (fetch_rc == -1) {
        return -1;
    } 
    if (fetch_rc == 0) {
        *offset += amount_read;
        if (sendToNessieFake(header->magic, header->opcode, header->keylength, 
                header->totalbody, header->extralength, 
                (char*)(&(header->opaque)), req.body) == -1) {
            delete[] req.body;
            return -1;
        }
        delete[] req.body;
        return 0;
    } else if (fetch_rc == 1) {
        *offset = 0;
        return 1;    
    }
    assert(false);
    return -1;
}
    
// TODO: This is fairly inefficient as it requires a memmove for every
//       request. Should be based on offsets.    
int Connection::fetch_request(struct Request* req, int offset, int* amount_read) {       
    // Check if we have received the entire header
    if (buf_used_- offset < (int)sizeof(struct RequestHeader)) {
        if (buf_used_ - offset > 0) {
            memmove(buf_, buf_ + offset, buf_used_ - offset);
        }
        buf_used_ = buf_used_ - offset;
        return 1;
    }
    struct RequestHeader* head = reinterpret_cast<RequestHeader*>(buf_ + offset);                
    if (head->magic != 0x80) {
        return -1; // Need to close connection.
    }
    int keylength = ntohs(head->keylength);
    int totalbody = ntohl(head->totalbody);            
    // Check if we have retrieved the entire data
    int request_size = totalbody + sizeof(struct RequestHeader);
    if (buf_used_ - offset >= request_size) {
        // TODO: Dynamic allocation overhead can be removed.
        assert(totalbody < 100000);
        char* body = new char[totalbody];
        // Copy the body and the header separately
        memcpy(body, buf_ + offset + sizeof(struct RequestHeader), totalbody);
        memcpy(&(req->header), head, sizeof(struct RequestHeader));
        req->body = body;
        //buf_used_ -= request_size;
        //if (buf_used_ > 0) {
        //    memmove(buf_, buf_ + request_size, buf_used_);
        //}
        *amount_read = request_size;
        return 0;
    }
    if (buf_used_ - offset > 0) {
        memmove(buf_, buf_ + offset, buf_used_ - offset);
    }
    buf_used_ = buf_used_ - offset;
    return 1;
}
    
void Connection::copy_to_write_buf(int offset, char* src, int src_len) {
    out_bufs_.push_back("");
    out_bufs_.back().assign(&(src[offset]), src_len - offset);        
}
    
int Connection::sendToNessieFake(int magic, int opcode, int keylength, 
                                 int totalbody, int extralength, char* opaque, 
                                 char* buffer) {
    // do stuff with nessie
    if (opcode == 0) { // GET
        char notfound[33] = {0};
        notfound[0] = 0x81;
        notfound[1] = opcode;
        notfound[7] = 0x1;
        notfound[11] = 0x9;
        notfound[12] = opaque[0];
        notfound[13] = opaque[1];
        notfound[14] = opaque[2];
        notfound[15] = opaque[3];

        notfound[24] = 0x4e;
        notfound[25] = 0x6f;
        notfound[26] = 0x74;
        notfound[27] = 0x20;
        notfound[28] = 0x66;
        notfound[29] = 0x6f;
        notfound[30] = 0x75;
        notfound[31] = 0x6e;
        notfound[32] = 0x64;
        
        if (out_bufs_.empty()) {
            int n = send(sock_, notfound, sizeof(notfound), 0);                
            if (n == -1) {
                return -1;    
            } else if ((unsigned)n < sizeof(notfound)) {
                copy_to_write_buf(n, notfound, sizeof(notfound));
            }
        } else {
           copy_to_write_buf(0, notfound, sizeof(notfound));
        }
    } else { // SET
        char response[24] = {0};
        memset(response, 0, 24* sizeof(char));
        response[0] = 0x81;
        response[1] = opcode;
        response[12] = opaque[0];
        response[13] = opaque[1];
        response[14] = opaque[2];
        response[15] = opaque[3];        
        if (out_bufs_.empty()) {            
            int n = send(sock_, response, sizeof(response), 0);
            if (n == -1) {
                return -1;    
            } else if ((unsigned)n < sizeof(response)) {
                copy_to_write_buf(n, response, sizeof(response));
            }
        } else {
            copy_to_write_buf(0, response, sizeof(response));            
        }
    }
    return 0;
}    
    
int Connection::flush_writes() {
    while (!out_bufs_.empty()) {
        int str_size = out_bufs_.front().size();
        int n = send(sock_, out_bufs_.front().data(), str_size, 0);
        if (n == -1) {
            return -1;
        } else if (n < str_size) {
            out_bufs_.front().erase(0, n);
            return 1;
        }
        out_bufs_.pop_front();
    }
    return 0;
}    
    
bool Connection::pending_writes_exist() {
    return !out_bufs_.empty();
}
    
int Connection::handle_request() {
    while (true) {            
        // First try to fill the read buffer
        int fill_rc = fill_buffer();
        if (fill_rc == -1) {
            return -1; // Error with the read
        }
        // Fetch requests from the buffer
        int offset = 0;     
        while (true) {
            int process_rc = process_next_request(&offset);
            if (process_rc == -1) {
                return -1;                    
            } else if (process_rc == 0) {
                continue; // Try to process the next request
            } else if (process_rc == 1) {
                if (fill_rc == 0) {
                    // Read socket buffer has been drained. No more
                    // to read. Just return.
                    return out_bufs_.empty() ? 0 : 1;                        
                } else if (fill_rc == 1) {
                    break; // Try to read more from the socket.    
                } else {
                    assert(false);
                    return -1;
                }
            } else {
                assert(false);
                return -1;
            }
        }
    }        
}
}
