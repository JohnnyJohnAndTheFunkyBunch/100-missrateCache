  
#include <stdio.h>
#include <string.h>   //strlen
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>   //close
#include <arpa/inet.h>    //close
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h> //FD_SET, FD_ISSET, FD_ZERO macros
#include <sys/uio.h> 
#include <assert.h> 
#include <pthread.h> 
#include <queue> 
#include <netinet/tcp.h>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <climits>

#include <common/compatibility.hpp>
#include <common/custom_exception.hpp>
#include <nessie/nessie.hpp>
#include <nessie/nessie_config.hpp>
#include <nessie/nessie_key.hpp>
#include <nessie/nessie_value.hpp>
  
#define TRUE   1
#define FALSE  0
#define PORT 11211
#define MAX_CONNECTIONS 200
#define BUFFER_LENGTH 4096
#define NUM_WORKER 8

using namespace std;

pthread_mutex_t lock;
pthread_cond_t cond;
pthread_t workers[NUM_WORKER];

Nessie *nessie;
std::fstream output;

bool nessie_shutdown = false;

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
    RequestHeader header;
    char* body;
};

std::queue<Request> task_queue;

void sendToNessieFake(int magic, int opcode, int keylength, int totalbody, int extralength, char* opaque, char* buffer, int csock);

int setNonblocking(int fd)
{
    int flags;

    /* If they have O_NONBLOCK, use the Posix way to do it */
#if defined(O_NONBLOCK)
    /* Fixme: O_NONBLOCK is defined but broken on SunOS 4.1.x and AIX 3.2.5. */
    if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
        flags = 0;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#else
    /* Otherwise, use the old way of doing it */
    flags = 1;
    return ioctl(fd, FIOBIO, &flags);
#endif
}    

template <typename T>
T swap_endian(T u)
{
    static_assert (CHAR_BIT == 8, "CHAR_BIT != 8");

    union
    {
        T u;
        unsigned char u8[sizeof(T)];
    } source, dest;

    source.u = u;

    for (size_t k = 0; k < sizeof(T); k++)
        dest.u8[k] = source.u8[sizeof(T) - k - 1];

    return dest.u;
}

void
hexdump(char* buffer, int length)
{
    /* We don't want to use isalpha here; setting the locale would change
 *      * which characters are considered alphabetical. */

  printf("Writing:\n");
  int i;
  for(i = 0; i < length; i++){
    printf( "%8x ", *((unsigned char*)buffer+i));
    if(i % 4 == 3 && i != 0){
     printf("\n");
    }
  }
  printf("\n");
}

class Connection {
private:
    int socket;
    char buf[BUFFER_LENGTH];
    int buflen;

public:
    Connection(int socket) : socket(socket), buflen(0) {}

    int get_sock() { return socket; }
    int Close() { close(socket); }

    // NOTE: Assuming that a structure always start at the beginning
    //       of the buffer. To make this more efficient, either have
    //       a start offset, or make this into a circular buffer (this
    //       is much more complex).
    int fetch_request(Request *req) {
        int recv_len = recv(socket, buf + buflen, BUFFER_LENGTH - buflen, 0);
        
        if (recv_len == -1) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                return 1; // Cannot currently read any data from the socket
            } else {
                return -1;
            }
        } else if (recv_len == 0) {
            return 1; // Cannot currently read any data from the socket
        } 
        buflen += recv_len;
        if (buflen < sizeof(struct RequestHeader)) {
            // Have not read enough to read the header, continue;
            return 1;
        }
        struct RequestHeader head;
        memcpy(&head, buf, sizeof(struct RequestHeader));
        assert(head.magic == 0x80);
        head.keylength = swap_endian<uint8_t>(head.keylength);
        head.totalbody = swap_endian<uint32_t>(head.totalbody);
        head.extralength = swap_endian<uint8_t>(head.extralength);
        // Check if we have retrieved the entire data
        if (buflen >= head.totalbody + sizeof(struct RequestHeader)) {
            char* body = new char[head.totalbody];
            memcpy(body, buf + sizeof(struct RequestHeader), head.totalbody);
            req->header = head;
            req->body = body;
            buflen -= (head.totalbody + sizeof(struct RequestHeader));
            memmove(buf, buf + ((head.totalbody + sizeof(struct RequestHeader)) * sizeof(char)), buflen);
            return 0;
        } else {
           return 1;
        }
    }

    int handle_request(const Request &req) {
        RequestHeader head = req.header;
        sendToNessieFake(head.magic, head.opcode, head.keylength, head.totalbody, 
                        head.extralength, (char*)(&(head.opaque)), req.body, socket);
        delete[] req.body;
        return 0;
    }
};




void printStats()
{
    while(true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        nessie->PrintStats();
    }
}
/*
void sendToNessie(int magic, int opcode, int keylength, int totalbody, int extralength, char* opaque, char* buffer, int csock, int worker_id = 0) {
    // do stuff with nessie
    if (opcode == 0) { // GET
        std::string key(buffer + extralength, keylength);
        NessieKey n_key(key);
        NessieValue n_get_value(nessie->Get(n_key, worker_id));
        if (n_get_value.Error() != NessieConfig::Error::NONE) {
            char notfound[33] = {0};
            notfound[0] = 0x81;
            notfound[1] = opcode;
            notfound[7] = 0x1;
            notfound[11] = 0x9;
            notfound[12] = opaque[0];
            notfound[13] = opaque[1];
            notfound[14] = opaque[2];
            notfound[15] = opaque[3];

            notfound[24] = 'N';
            notfound[25] = 'o';
            notfound[26] = 't';
            notfound[27] = ' ';
            notfound[28] = 'f';
            notfound[29] = 'o';
            notfound[30] = 'u';
            notfound[31] = 'n';
            notfound[32] = 'd';
            (void)write(csock, notfound, 33);
            return;
        }
        std::string value_str(n_get_value.Get());
        int value_size = value_str.size();
        int body_size = value_size + 4;
        char response[24+ value_size + 4];
        memset(response, 0, (24+value_size+4) * sizeof(char));
        response[0] = 0x81;
        response[1] = opcode;
        response[4] = 0x04;
        response[8] = ((unsigned int)(body_size & 0xff000000)) >> 24;
        response[9] = ((unsigned int)(body_size & 0xff0000)) >> 16;
        response[10] = ((unsigned int)(body_size & 0xff00)) >> 8;
        response[11] = (body_size & 0xff);
        response[12] = opaque[0];
        response[13] = opaque[1];
        response[14] = opaque[2];
        response[15] = opaque[3];
        response[23] = 0x01;
        response[24] = 0xde;
        response[25] = 0xad;
        response[26] = 0xbe;
        response[27] = 0xef;
        for (int i = 0; i < value_size; i++) {
            response[28 + i] = value_str[i];
        }
        (void)write(csock, response, 24+value_size+4);
    } else { // SET
        std::string key(buffer + extralength, keylength);
        std::string value(buffer + extralength + keylength, totalbody - extralength - keylength);
        NessieKey n_key(key);
        NessieValue n_value(value);
        NessieConfig::Error n_error(nessie->Put(n_key, n_value, worker_id));
        if (n_error != NessieConfig::Error::NONE) {
            char response[24] = {0};
            response[0] = 0x81;
            response[1] = opcode;
            response[12] = opaque[0];
            response[13] = opaque[1];
            response[14] = opaque[2];
            response[15] = opaque[3];
            (void)write(csock, response, 24);
            return;
        }
        char response[24] = {0};
        response[0] = 0x81;
        response[1] = opcode;
        response[12] = opaque[0];
        response[13] = opaque[1];
        response[14] = opaque[2];
        response[15] = opaque[3];
        (void)write(csock, response, 24);
    }
}
*/

static void NessieInit(void)
{
    std::string config = "config/memcached/nessie.cfg";
    output.open("regtest.out");
    nessie = new Nessie(output,
        0, config);
    nessie->Initialize();
    nessie->Start();
	return;
}
/*
void *worker_function(void *args)
{
    Request myReq;
    int thread_id = *(int*)(args);
    delete args;
    while (!nessie_shutdown) {
        pthread_mutex_lock(&lock);
        while(task_queue.empty()) {
            pthread_cond_wait(&cond, &lock);
        }
        myReq = task_queue.front();
        task_queue.pop();
        pthread_mutex_unlock(&lock);
        // do something with request
        sendToNessie(myReq.magic, myReq.opcode, myReq.keylength, myReq.totalbody, myReq.extralength,
                         myReq.opaque, myReq.buffer, myReq.csock, thread_id);
        // Scary pointer math. What is 24? Avoid magic numbers
        delete[] (myReq.buffer - 24);
    }
}
*/

void sendToNessieFake(int magic, int opcode, int keylength, int totalbody, int extralength, char* opaque, char* buffer, int csock) {
    // do stuff with nessie
    int n = -1;
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
        n = write(csock, notfound, 33);
        if (n < 0) {
            perror("write");
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
        n = write(csock, response, 24);
        if (n < 0) {
            perror("write");
        }
    }
}


int process_request(Connection* new_conn, map<int, Connection*>* socket_map) {
    Request req;
    int ret_val = new_conn->fetch_request(&req);
    if (ret_val == 0) {
        // Full request has been retrieved.
        // Push into request data structure.
        // In current case, process request here.
        new_conn->handle_request(req);
        return 0;
    } 
    if (ret_val != 1) {
        // Error retrieving data from the connection. Close connection.
        socket_map->erase(new_conn->get_sock());
        new_conn->Close();
        delete new_conn;
        return -1;
    }
    return 1; // More data to read
}

int process_response(Connection* new_conn, map<int, Connection*>* socket_map) {
}

 
int main(int argc , char *argv[])
{
    int opt = TRUE;
    int master_socket , addrlen , new_socket , client_socket[MAX_CONNECTIONS] , max_clients = MAX_CONNECTIONS , activity, i , r , sd;
    int max_sd;
    struct sockaddr_in address;

    map<int, Connection*> socket_map;
      
    //char buffer[BUFFER_LENGTH * MAX_CONNECTIONS];  //data buffer of 1K
    //int offset[MAX_CONNECTIONS] = {0};
      
    //set of socket descriptors
    fd_set fds;
  
    //initialise all client_socket[] to 0 so not checked
    for (i = 0; i < max_clients; i++) 
    {
        client_socket[i] = 0;
    }
      
    //create a master socket
    if( (master_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0) 
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
  
    //set master socket to allow multiple connections , this is just a good habit, it will work without this
    if( setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 )
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
  
    //type of socket created
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons( PORT );
      
    //bind the socket to localhost port 8888
    if (bind(master_socket, (struct sockaddr *)&address, sizeof(address))<0) 
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    setNonblocking(master_socket);
    printf("Listener on port %d \n", PORT);
     
    //try to specify maximum of 3 pending connections for the master socket
    //TODO: backlog of only 3 is on the low side. This number is somewhat arbitary
    //      However, most systems have at least 10 or more. In practice, there is
    //      no reason not to increase this to, say, 100.
    if (listen(master_socket, 100) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }
      
    //accept the incoming connection
    addrlen = sizeof(address);
    puts("Waiting for connections ...");

    /* Remove worker threads for now
    
    // initialize worker Threads
    for (int i = 0; i < NUM_WORKER; i++) 
    {
        int *thread_id = new int;
        *thread_id = i;
        int s = pthread_create(&workers[i], NULL,
                                  worker_function, (void*)thread_id);
        if (s != 0) {
            perror("pthread_create");
            exit(EXIT_FAILURE);
        }
    }
    */

    // initialize nessie
    //NessieInit();

    // initailize print loop
    //std::thread printLoop(printStats);
     
    //clear the socket set
    FD_ZERO(&fds);
  
    //add master socket to set
    FD_SET(master_socket, &fds);

    // This is currently our largest fd
    max_sd = master_socket;

    while(TRUE) 
    {

        // Make a copy of the readfds because select will overwrite the fdset.        
        fd_set working_readfds = fds;
        // fd_set working_writefds = fds; TODO: have select for writes
  
        //wait for an activity on one of the sockets , timeout is NULL , so wait indefinitely
        activity = select( max_sd + 1 , &working_readfds , &working_writefds , NULL , NULL);
    
        if ((activity < 0) && (errno!=EINTR)) 
        {
            printf("select error");
            exit(EXIT_FAILURE);
        }
          
        //If something happened on the master socket , then its an incoming connection
        if (FD_ISSET(master_socket, &working_readfds)) 
        {
            if ((new_socket = accept(master_socket, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0)
            {
                perror("accept");
                exit(EXIT_FAILURE);
            }
            // Consider making the server socket also nonblocking. This is probably more
            // complicated than it needs to be, but let's assume it is correct for now.
            /*
            int j = 1;
            if (fcntl(new_socket, F_SETFL, fcntl(new_socket, F_GETFL) | O_NONBLOCK) < 0) {
                perror("setting O_NONBLOCK");
                close(new_socket);
                exit(EXIT_FAILURE);
            }
            */
            setNonblocking(new_socket);
            FD_SET(new_socket, &fds);

            Connection* new_conn = new Connection(new_socket);

            socket_map[new_socket] = new_conn;

            int ret_val = process_request(new_conn, &socket_map);
            if (ret_val == -1) {
                FD_CLR(new_socket, &fds);
            }
        }
        for (i = 0; i < max_sd + 1; i++) {
            if (!FD_ISSET(i, &working_readfds) || i == master_socket) {
                continue; // Skip these sockets
            }
            Connection* new_conn = socket_map[i];
            assert(new_conn != NULL);
            int ret_val = process_request(new_conn, &socket_map);
            if (ret_val == -1) {
                FD_CLR(i, &fds);
            }
        }
        if (new_socket > max_sd)
            max_sd = new_socket;
    }
      
    return 0;
} 
