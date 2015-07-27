#ifndef SOCKET_UTIL_H
#define SOCKET_UTIL_H

#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

namespace NessieCache {

class SocketUtil {
  public:
    static int set_non_block(int fd) {
        int flags;    
        /* If they have O_NONBLOCK, use the Posix way to do it */
#if defined(O_NONBLOCK)
        /* Fixme: O_NONBLOCK is defined but broken on SunOS 4.1.x and AIX 3.2.5. */
        if (-1 == (flags = fcntl(fd, F_GETFL, 0))) {
            flags = 0;
        }
        return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#else
        /* Otherwise, use the old way of doing it */
        flags = 1;
        return ioctl(fd, FIOBIO, &flags);
#endif
    }
    static void turn_off_nagle(int fd) {
        int flag = 1;
        int result = setsockopt(fd,            /* socket affected */
                                IPPROTO_TCP,     /* set option at TCP level */
                                TCP_NODELAY,     /* name of option */
                                (char *) &flag,  /* the cast is historical cruft */
                                sizeof(int));
        
    }
};
}
#endif
