/*
 * CS 371 PA2 Task 1: UDP-based Pipelined Protocol
 * Assigned: Feb 27, 2026  Due: Mar 09, 2026
 *
 * Group members (optional):
 *   Student #1:
 *   Student #2:
 *   Student #3:
 *
 * - UDP sockets (no TCP)
 * - Pipelined: multiple in-flight packets per client
 * - Metrics: tx_cnt, rx_cnt, lost_pkt_cnt = tx_cnt - rx_cnt
 * - Per-packet timeout to detect loss
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <errno.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define PAYLOAD_SIZE 12
#define SEQ_SIZE 4
#define DEFAULT_CLIENT_THREADS 4
#define PIPELINE_WINDOW 64
#define TIMEOUT_MS 200

static char *server_ip = "127.0.0.1";
static int server_port = 12345;
static int num_client_threads = DEFAULT_CLIENT_THREADS;
static int num_requests = 100000;

typedef struct {
    uint32_t seq;
    struct timeval send_time;
} in_flight_t;

typedef struct {
    int epoll_fd;
    int socket_fd;
    struct sockaddr_in server_addr;
    socklen_t server_len;
    long long tx_cnt;
    long long rx_cnt;
    long long lost_pkt_cnt;
    uint32_t next_seq;
    in_flight_t in_flight[PIPELINE_WINDOW];
    int in_flight_count;
} client_thread_data_t;

static inline void put_seq(char *buf, uint32_t seq) {
    uint32_t n = htonl(seq);
    memcpy(buf, &n, SEQ_SIZE);
}

static inline uint32_t get_seq(const char *buf) {
    uint32_t n;
    memcpy(&n, buf, SEQ_SIZE);
    return ntohl(n);
}

static void client_send_packet(client_thread_data_t *data, uint32_t seq) {
    char send_buf[MESSAGE_SIZE];
    memset(send_buf, 0, MESSAGE_SIZE);
    put_seq(send_buf, seq);
    memcpy(send_buf + SEQ_SIZE, "ABCDEFGHIJKL", PAYLOAD_SIZE);

    ssize_t n = sendto(data->socket_fd, send_buf, MESSAGE_SIZE, 0,
                       (struct sockaddr *)&data->server_addr, data->server_len);
    if (n != MESSAGE_SIZE) {
        if (n < 0)
            perror("sendto");
        /* still count as sent for loss metrics */
    }
    data->tx_cnt++;
    data->in_flight[data->in_flight_count].seq = seq;
    gettimeofday(&data->in_flight[data->in_flight_count].send_time, NULL);
    data->in_flight_count++;
}

static int client_remove_acked(client_thread_data_t *data, uint32_t seq) {
    int i, j;
    for (i = 0; i < data->in_flight_count; i++) {
        if (data->in_flight[i].seq == seq) {
            for (j = i; j < data->in_flight_count - 1; j++)
                data->in_flight[j] = data->in_flight[j + 1];
            data->in_flight_count--;
            return 1;
        }
    }
    return 0;
}

static int client_check_timeout(client_thread_data_t *data) {
    struct timeval now;
    gettimeofday(&now, NULL);
    long timeout_us = (long)TIMEOUT_MS * 1000;

    if (data->in_flight_count == 0)
        return 0;
    /* Find the oldest packet by send_time (in_flight order is not by time) */
    int oldest = 0;
    long max_elapsed = (now.tv_sec - data->in_flight[0].send_time.tv_sec) * 1000000L
                       + (now.tv_usec - data->in_flight[0].send_time.tv_usec);
    for (int k = 1; k < data->in_flight_count; k++) {
        long el = (now.tv_sec - data->in_flight[k].send_time.tv_sec) * 1000000L
                  + (now.tv_usec - data->in_flight[k].send_time.tv_usec);
        if (el > max_elapsed) {
            max_elapsed = el;
            oldest = k;
        }
    }
    if (max_elapsed < timeout_us)
        return 0;
    /* Remove the oldest (timed-out) packet */
    for (int j = oldest; j < data->in_flight_count - 1; j++)
        data->in_flight[j] = data->in_flight[j + 1];
    data->in_flight_count--;
    return 1;
}

void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event, events[MAX_EVENTS];
    char recv_buf[MESSAGE_SIZE];
    struct sockaddr_in from;
    socklen_t from_len = sizeof(from);

    data->tx_cnt = 0;
    data->rx_cnt = 0;
    data->next_seq = 0;
    data->in_flight_count = 0;

    event.events = EPOLLIN;
    event.data.fd = data->socket_fd;
    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event) != 0) {
        perror("epoll_ctl ADD");
        close(data->socket_fd);
        close(data->epoll_fd);
        return NULL;
    }

    while (data->tx_cnt < num_requests || data->in_flight_count > 0) {
        /* fill pipeline */
        while (data->in_flight_count < PIPELINE_WINDOW && data->tx_cnt < num_requests) {
            client_send_packet(data, data->next_seq);
            data->next_seq++;
        }

        int wait_ms = 10;
        int n_events = epoll_wait(data->epoll_fd, events, MAX_EVENTS, wait_ms);
        if (n_events < 0 && errno != EINTR) {
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n_events; i++) {
            if (events[i].data.fd != data->socket_fd)
                continue;
            ssize_t n = recvfrom(data->socket_fd, recv_buf, MESSAGE_SIZE, 0,
                                 (struct sockaddr *)&from, &from_len);
            if (n != MESSAGE_SIZE)
                continue;
            uint32_t seq = get_seq(recv_buf);
            if (client_remove_acked(data, seq))
                data->rx_cnt++;
        }

        /* time out oldest in-flight packet if needed */
        while (client_check_timeout(data))
            ;
    }

    data->lost_pkt_cnt = data->tx_cnt - data->rx_cnt;

    close(data->socket_fd);
    close(data->epoll_fd);
    return NULL;
}

static void run_client(void) {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];
    struct sockaddr_in server_addr;

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) != 1) {
        perror("inet_pton");
        return;
    }

    for (int i = 0; i < num_client_threads; i++) {
        thread_data[i].epoll_fd = epoll_create1(0);
        if (thread_data[i].epoll_fd < 0) {
            perror("epoll_create1");
            for (int j = 0; j < i; j++) {
                close(thread_data[j].socket_fd);
                close(thread_data[j].epoll_fd);
            }
            return;
        }
        thread_data[i].socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (thread_data[i].socket_fd < 0) {
            perror("socket");
            close(thread_data[i].epoll_fd);
            for (int j = 0; j < i; j++) {
                close(thread_data[j].socket_fd);
                close(thread_data[j].epoll_fd);
            }
            return;
        }
        memcpy(&thread_data[i].server_addr, &server_addr, sizeof(server_addr));
        thread_data[i].server_len = sizeof(server_addr);
    }

    for (int i = 0; i < num_client_threads; i++) {
        if (pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]) != 0) {
            perror("pthread_create");
            return;
        }
    }

    long long total_tx = 0, total_rx = 0, total_lost = 0;
    for (int i = 0; i < num_client_threads; i++) {
        if (pthread_join(threads[i], NULL) != 0)
            perror("pthread_join");
        total_tx += thread_data[i].tx_cnt;
        total_rx += thread_data[i].rx_cnt;
        total_lost += thread_data[i].lost_pkt_cnt;
        printf("Thread %d: tx_cnt=%lld rx_cnt=%lld lost_pkt_cnt=%lld\n",
               i, thread_data[i].tx_cnt, thread_data[i].rx_cnt, thread_data[i].lost_pkt_cnt);
    }
    printf("Total: tx_cnt=%lld rx_cnt=%lld lost_pkt_cnt=%lld\n", total_tx, total_rx, total_lost);
}

static void run_server(void) {
    int server_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(server_port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
        perror("bind");
        close(server_fd);
        return;
    }

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1");
        close(server_fd);
        return;
    }

    struct epoll_event event, events[MAX_EVENTS];
    event.events = EPOLLIN;
    event.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) != 0) {
        perror("epoll_ctl ADD");
        close(epoll_fd);
        close(server_fd);
        return;
    }

    char buffer[MESSAGE_SIZE];
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    while (1) {
        int n_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (n_events < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n_events; i++) {
            if (events[i].data.fd != server_fd)
                continue;
            ssize_t n = recvfrom(server_fd, buffer, MESSAGE_SIZE, 0,
                                (struct sockaddr *)&client_addr, &client_len);
            if (n != MESSAGE_SIZE)
                continue;
            ssize_t sent = sendto(server_fd, buffer, MESSAGE_SIZE, 0,
                                  (struct sockaddr *)&client_addr, client_len);
            if (sent != MESSAGE_SIZE) {
                if (sent < 0)
                    perror("sendto");
            }
        }
    }

    close(epoll_fd);
    close(server_fd);
}

int main(int argc, char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "server") == 0) {
        if (argc > 2)
            server_ip = argv[2];
        if (argc > 3)
            server_port = atoi(argv[3]);
        run_server();
    } else if (argc > 1 && strcmp(argv[1], "client") == 0) {
        if (argc > 2)
            server_ip = argv[2];
        if (argc > 3)
            server_port = atoi(argv[3]);
        if (argc > 4)
            num_client_threads = atoi(argv[4]);
        if (argc > 5)
            num_requests = atoi(argv[5]);
        if (num_client_threads <= 0 || num_requests <= 0) {
            fprintf(stderr, "Error: num_client_threads and num_requests must be positive\n");
            return 1;
        }
        run_client();
    } else {
        printf("Usage: %s <server|client> <server_ip> <server_port> <num_client_threads> <num_requests>\n", argv[0]);
        return 1;
    }
    return 0;
}
