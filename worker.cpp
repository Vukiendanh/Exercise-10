#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <cstring>
#include "../common/protocol.h"

// Hàm tính giai thừa
long long compute_factorial(long long n) {
    if (n <= 0) return 1;
    long long res = 1;
    for (int i = 1; i <= n; ++i) res *= i;
    return res;
}

// Hàm đếm số nguyên tố (Thuật toán kiểm tra căn bậc hai tối ưu)
long long count_primes(long long n) {
    long long count = 0;
    for (long long i = 2; i <= n; ++i) {
        bool is_prime = true;
        for (long long j = 2; j * j <= i; ++j) {
            if (i % j == 0) {
                is_prime = false;
                break;
            }
        }
        if (is_prime) count++;
    }
    return count;
}

// Luồng gửi Heartbeat ngầm (Mỗi 2 giây)
void heartbeat_sender(int sock, int worker_id) {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        Message msg;
        msg.type = HEARTBEAT;
        msg.worker_id = worker_id;
        if (send(sock, &msg, sizeof(Message), 0) <= 0) break;
    }
}

int main(int argc, char* argv[]) {
    int worker_id = (argc > 1) ? std::stoi(argv[1]) : 1;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(8080);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Ket noi den Master that bai!\n";
        return -1;
    }

    std::cout << "[WORKER " << worker_id << "] Da ket noi den Master.\n";

    // Gửi gói tin ĐĂNG KÝ
    Message reg_msg;
    reg_msg.type = REGISTER;
    reg_msg.worker_id = worker_id; 
    send(sock, &reg_msg, sizeof(Message), 0);

    std::thread hb_thread(heartbeat_sender, sock, worker_id);
    hb_thread.detach();

    Message recv_msg;
    while (true) {
        int valread = recv(sock, &recv_msg, sizeof(Message), 0);
        if (valread <= 0) break;

        if (recv_msg.type == TASK_ASSIGN) {
            long long result = 0;
            
            if (std::strcmp(recv_msg.operation, "factorial") == 0) {
                result = compute_factorial(recv_msg.input);
            } 
            else if (std::strcmp(recv_msg.operation, "prime") == 0) {
                result = count_primes(recv_msg.input);
            }

            // Trả kết quả về
            Message res_msg;
            res_msg.type = TASK_RESULT;
            res_msg.worker_id = worker_id;
            res_msg.task_id = recv_msg.task_id;
            res_msg.output = result;

            send(sock, &res_msg, sizeof(Message), 0);
        }
    }

    close(sock);
    return 0;
}
