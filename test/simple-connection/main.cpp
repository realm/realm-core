#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

char time_buffer[1000];

void get_time(char* time_buffer)
{
    time_t rawtime;
    time(&rawtime);

    struct tm* time_info;
    time_info = localtime(&rawtime);

    sprintf(time_buffer, "%s", asctime(time_info));
}

int connect_to_server()
{
    int sock_fd = socket(PF_INET, SOCK_STREAM, 0);

    if (sock_fd == -1) {
        printf("error in socket: %s\n", strerror(errno));
        return -1;
    }

    struct sockaddr_in sa;

    const char* ip_addr = "10.60.101.153";
    uint16_t port = 27800;

    memset(&sa, 0, sizeof(struct sockaddr_in));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = inet_addr(ip_addr);
    sa.sin_port = htons(port);

    int rc = connect(sock_fd, (struct sockaddr*)&sa, sizeof(sa));
    if (rc == -1) {
        printf("Error in connect: %s\n", strerror(errno));
        return -2;
    }

    const char request[] = "GET /realm-object-server HTTP/1.1\r\n"
                           "Host: lt.sync.realmlab.net\r\n"
                           "Upgrade: websocket\r\n"
                           "Connection: Upgrade\r\n"
                           "Sec-WebSocket-Key: EYRk0diT0W7xxd/jbitJJw==\r\n"
                           "Sec-WebSocket-Version: 13\r\n"
                           "Sec-WebSocket-Protocol: io.realm.protocol\r\n"
                           "\r\n";

    ssize_t bytes_written = write(sock_fd, request, sizeof(request) - 1);
    if (bytes_written != sizeof(request) - 1) {
        close(sock_fd);
        return -3;
    }

    const char* exp_resp_head = "HTTP/1.1 101 Switching Protocols\r\n";
    const size_t exp_resp_head_size = sizeof(exp_resp_head) - 1;

    char response[1000];
    ssize_t bytes_read = read(sock_fd, response, 1000);
    if (bytes_read < exp_resp_head_size) {
        close(sock_fd);
        return -4;
    }
    // printf("bytes read = %zd\n\n", bytes_read);
    // printf("%s\n", response);

    if (strncmp(response, exp_resp_head, exp_resp_head_size) != 0) {
        printf("incorrect response\n");
        close(sock_fd);
        return -4;
    }

    // rc = close(sock_fd);
    // if (rc == -1) {
    //	printf("Error in close: %s\n", strerror(errno));
    //	return -5;
    //}

    return sock_fd;
}

int multiple_connect(int nconn)
{
    for (int i = 0; i < nconn; ++i) {
        int fd = connect_to_server();
        if (fd < 0) {
            printf("pid = %d, Connection error, fd = %d\n", getpid(), fd);
            return 1;
        }
        if (i % 100 == 0) {
            get_time(time_buffer);
            printf("pid = %d, i = %d, fd = %d time = %s", getpid(), i, fd, time_buffer);
        }
    }

    get_time(time_buffer);
    printf("pid = %d, goes to sleep, time = %s", getpid(), time_buffer);
    sleep(1000000);

    return 0;
}

int main(int argc, char** argv)
{
    int nproc = atoi(argv[1]);
    int nconn = atoi(argv[2]);

    for (int i = 0; i < nproc; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            multiple_connect(nconn);
            return 1;
        }
    }

    printf("parent, pid = %d, goes to sleep\n", getpid());
    sleep(1000000);

    return 0;
}
