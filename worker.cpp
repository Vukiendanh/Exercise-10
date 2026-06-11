#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <cstring>
#include "../common/protocol.h"

// ============================================================================
// CÁC HÀM TÍNH TOÁN (XỬ LÝ TÁC VỤ CPU-INTENSIVE)
// ============================================================================

// Hàm tính giai thừa (Factorial)
long long compute_factorial(long long n) {
    if (n <= 0) return 1;
    long long res = 1;
    for (int i = 1; i <= n; ++i) {
        res *= i;
    }
    return res;
}

// Hàm kiểm tra và đếm số lượng số nguyên tố từ 2 đến N (Prime Counter)
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

// ============================================================================
// THREAD 2: LUỒNG CHẠY NGẦM GỬI HEARTBEAT ĐỊNH KỲ (MỖI 2 GIÂY)
// ============================================================================
void heartbeat_sender(int sock, int worker_id) {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2)); // Chờ 2 giây
        
        Message msg;
        msg.type = HEARTBEAT;
        msg.worker_id = worker_id;
        
        // Gửi gói tin Heartbeat qua mạng TCP
        if (send(sock, &msg, sizeof(Message), 0) <= 0) {
            std::cout << "[WORKER] Mat ket noi voi Master. Dung luong Heartbeat!\n";
            break;
        }
    }
}

// ============================================================================
// THREAD 1 (MAIN THREAD): KẾT NỐI, ĐĂNG KÝ VÀ LẮNG NGHE NHẬN TÁC VỤ (RECEIVE TASK)
// ============================================================================
int main(int argc, char* argv[]) {
    // Cho phép cấu hình ID của Worker thông qua tham số dòng lệnh (Ví dụ: ./worker 2)
    // Nếu không truyền tham số, mặc định ID = 1
    int worker_id = (argc > 1) ? std::stoi(argv[1]) : 1;

    // 1. Khởi tạo Socket TCP Client
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Khong the tao socket cho Worker!\n";
        return -1;
    }

    sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(8080); // Kết nối đến cổng 8080 của Master
    
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        std::cerr << "Dia chi IP cua Master khong hop le!\n";
        close(sock);
        return -1;
    }

    // Kết nối tới Master Node
    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Ket noi den Master Node that bai!\n";
        close(sock);
        return -1;
    }

    std::cout << "[WORKER " << worker_id << "] Da ket noi den Master thanh cong.\n";

    // 2. Gửi gói tin ĐĂNG KÝ (REGISTER) đầu tiên ngay khi khởi động
    Message reg_msg;
    reg_msg.type = REGISTER;
    reg_msg.worker_id = worker_id; 
    send(sock, &reg_msg, sizeof(Message), 0);
    std::cout << "[WORKER " << worker_id << "] Da gui thong diep REGISTER.\n";

    // 3. Kích hoạt Luồng 2 chạy ngầm chuyên trách việc gửi Heartbeat
    std::thread hb_thread(heartbeat_sender, sock, worker_id);
    hb_thread.detach(); // Tách luồng độc lập

    // 4. Vòng lặp luồng chính: Liên tục chờ nhận Tác vụ (Task Assignment) từ Master
    Message recv_msg;
    while (true) {
        int valread = recv(sock, &recv_msg, sizeof(Message), 0);
        
        // Nếu ngắt kết nối với Master
        if (valread <= 0) {
            std::cout << "[WORKER " << worker_id << "] Master da ngat ket noi socket hoặc phat sinh loi mạng!\n";
            break;
        }

        // Nếu nhận được gói tin giao việc từ Master
        if (recv_msg.type == TASK_ASSIGN) {
            std::cout << "\n[WORKER " << worker_id << "] Nhan Task " << recv_msg.task_id 
                      << " [" << recv_msg.operation << "] voi Input: " << recv_msg.input << "\n";
            
            long long result = 0;
            
            // Tiến hành phân loại tác vụ để tính toán
            if (std::strcmp(recv_msg.operation, "factorial") == 0) {
                std::cout << "[WORKER " << worker_id << "] Dang tinh giai thua...\n";
                result = compute_factorial(recv_msg.input);
            } 
            else if (std::strcmp(recv_msg.operation, "prime") == 0) {
                std::cout << "[WORKER " << worker_id << "] Dang dem so nguyen to...\n";
                result = count_primes(recv_msg.input);
            } 
            else {
                std::cout << "[WORKER " << worker_id << "] Tac vu khong hop le!\n";
            }

            // Giả lập thời gian xử lý thực tế một chút để dễ quan sát tiến trình
            std::this_thread::sleep_for(std::chrono::seconds(2));

            // Đóng gói thông điệp trả về kết quả (TASK_RESULT)
            Message res_msg;
            res_msg.type = TASK_RESULT;
            res_msg.worker_id = worker_id;
            res_msg.task_id = recv_msg.task_id;
            res_msg.output = result;

            // Gửi trả kết quả về cho Master Node
            send(sock, &res_msg, sizeof(Message), 0);
            std::cout << "[WORKER " << worker_id << "] Da hoan thanh Task " << recv_msg.task_id 
                      << " và gui lai ket qua: " << result << "\n";
        }
    }

    close(sock);
    return 0;
}
