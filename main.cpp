  
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


struct Request {
    int magic;
    int opcode;
    int keylength;
    int totalbody;
    int extralength;
    char* opaque;
    char* buffer;
    int csock;
};

std::queue<Request> task_queue;
pthread_mutex_t lock;
pthread_cond_t cond;
pthread_t workers[NUM_WORKER];

Nessie *nessie;
std::fstream output;

bool nessie_shutdown = false;


void sendToNessieFake(int magic, int opcode, int keylength, int totalbody, int extralength, char* opaque, char* buffer, int csock);

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
        sendToNessieFake(myReq.magic, myReq.opcode, myReq.keylength, myReq.totalbody, myReq.extralength,
                         myReq.opaque, myReq.buffer, myReq.csock); //thread_id);
        delete[] (myReq.buffer - 24);
    }
}

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
        while(n < 0) {
            n = write(csock, notfound, 33);
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
        while(n < 0) {
            n = write(csock, response, 24);
        }
    }
}
 
int main(int argc , char *argv[])
{
    int opt = TRUE;
    int master_socket , addrlen , new_socket , client_socket[MAX_CONNECTIONS] , max_clients = MAX_CONNECTIONS , activity, i , r , sd;
    int max_sd;
    struct sockaddr_in address;
      
    char buffer[BUFFER_LENGTH * MAX_CONNECTIONS];  //data buffer of 1K
    int offset[MAX_CONNECTIONS] = {0};
      
    //set of socket descriptors
    fd_set readfds;
      
    //a message
    char *message = "ECHO Daemon v1.0 \r\n";
  
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
    printf("Listener on port %d \n", PORT);
     
    //try to specify maximum of 3 pending connections for the master socket
    if (listen(master_socket, 3) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }
      
    //accept the incoming connection
    addrlen = sizeof(address);
    puts("Waiting for connections ...");
    
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
    // initialize nessie
    NessieInit();
     
    while(TRUE) 
    {
        //clear the socket set
        FD_ZERO(&readfds);
  
        //add master socket to set
        FD_SET(master_socket, &readfds);
        max_sd = master_socket;
         
        //add child sockets to set
        for ( i = 0 ; i < max_clients ; i++) 
        {
            //socket descriptor
            sd = client_socket[i];
             
            //if valid socket descriptor then add to read list
            if(sd > 0)
                FD_SET( sd , &readfds);
             
            //highest file descriptor number, need it for the select function
            if(sd > max_sd)
                max_sd = sd;
        }
  
        //wait for an activity on one of the sockets , timeout is NULL , so wait indefinitely
        activity = select( max_sd + 1 , &readfds , NULL , NULL , NULL);
    
        if ((activity < 0) && (errno!=EINTR)) 
        {
            printf("select error");
        }
          
        //If something happened on the master socket , then its an incoming connection
        if (FD_ISSET(master_socket, &readfds)) 
        {
            if ((new_socket = accept(master_socket, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0)
            {
                perror("accept");
                exit(EXIT_FAILURE);
            }
            int j = 1;
            if (fcntl(new_socket, F_SETFL, fcntl(new_socket, F_GETFL) | O_NONBLOCK) < 0) {
                perror("setting O_NONBLOCK");
                close(new_socket);
                exit(EXIT_FAILURE);
            }
          
            //inform user of socket number - used in send and receive commands
        
            //send new connection greeting message
            /*
            if( send(new_socket, message, strlen(message), 0) != strlen(message) ) 
            {
                perror("send");
            }
              
            puts("Welcome message sent successfully");
            */
              
            //add new socket to array of sockets
            for (i = 0; i < max_clients; i++) 
            {
                //if position is empty
                if( client_socket[i] == 0 )
                {
                    client_socket[i] = new_socket;
                     
                    break;
                }
            }
        }
          
        //else its some IO operation on some other socket :)
        for (i = 0; i < max_clients; i++) 
        {
            sd = client_socket[i];
              
            if (FD_ISSET( sd , &readfds)) 
            {
                //Check if it was for closing , and also read the incoming message
                if ((r = read( sd , buffer + BUFFER_LENGTH * i + offset[i], BUFFER_LENGTH - offset[i])) == 0)
                {
                    //Somebody disconnected , get his details and print
                    getpeername(sd , (struct sockaddr*)&address , (socklen_t*)&addrlen);
                      
                    //Close the socket and mark as 0 in list for reuse
                    close( sd );
                    client_socket[i] = 0;
                }
                  
                //Send to nessie
                else
                {
                    char* mybuffer = buffer + BUFFER_LENGTH * i;
                    int magic;
                    int opcode;
                    int keylength;
                    int totalbody;
                    int extralength;
                    char* opaque;
                    r += offset[i];
                    while (r >= 24) {
                        magic = mybuffer[0] & 0xff;
                        assert(magic == 0x80);
                        opcode = mybuffer[1] & 0xff;
                        keylength = 0;
                        keylength |= mybuffer[3] & 0xff;
                        keylength |= (mybuffer[2] & 0xff) << 8;
                        extralength = mybuffer[4] & 0xff;
                        totalbody = 0;
                        totalbody |= mybuffer[11] & 0xff;
                        totalbody |= (mybuffer[10] & 0xff) << 8;
                        totalbody |= (mybuffer[9] & 0xff) << 16;
                        totalbody |= (mybuffer[8] & 0xff) << 24;
                        opaque = mybuffer + 12;
                        if (r >= totalbody + 24) {
                            char* newbuffer = new char[totalbody + 24];
                            memcpy(newbuffer, mybuffer, totalbody + 24);
                            Request req = {
                                .magic = magic,
                                .opcode = opcode,
                                .keylength = keylength,
                                .totalbody = totalbody,
                                .extralength = extralength,
                                .opaque = newbuffer + 12,
                                .buffer = newbuffer + 24,
                                .csock = sd};
                            // problem: mybuffer might change by the time
                            pthread_mutex_lock(&lock);
                            task_queue.push(req);
                            pthread_cond_signal (&cond);
                            pthread_mutex_unlock(&lock);
                            r -= (totalbody + 24);
                            memmove(mybuffer, mybuffer + ((totalbody + 24) * sizeof(char)), r);
                            offset[i] = r;
                        } else {
                            offset[i] = r;
                            break;
                        }
                    }
                }
            }
        }
    }
      
    return 0;
} 
