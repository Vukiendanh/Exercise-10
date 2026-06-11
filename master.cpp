#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <vector>
#include <mutex>
#include <thread>
#include <chrono>
#include <cstring>
#include "../common/protocol.h"

// Định nghĩa các cấu trúc dữ liệu lưu trữ nội bộ của Master theo yêu cầu đề bài
std::vector<WorkerInfo> worker_table; // Bảng quản lý Worker [cite: 60, 173]
std::vector<Task> task_queue;         // Hàng đợi quản lý Tác vụ [cite: 40, 173]

// Khóa Mutex để đồng bộ hóa dữ liệu giữa các luồng, tránh xung đột (Race Condition) [cite: 204, 205, 206]
std::mutex worker_mutex;
std::mutex task_mutex;

// ============================================================================
// THREAD 3: LUỒNG GIÁM SÁT HEARTBEAT & PHỤC HỒI LỖI (FAULT TOLERANCE)
// ============================================================================
void heartbeat_monitor() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1)); // Quét mỗi 1 giây [cite: 198]
        
        std::lock_guard<std::mutex> w_lock(worker_mutex);
        std::lock_guard<std::mutex> t_lock(task_mutex);
        
        time_t now = time(nullptr);
        for (auto& worker : worker_table) {
            // Phát hiện lỗi nếu Quá 6 giây không nhận được Heartbeat từ Worker đang sống [cite: 43, 157, 158, 159]
            if (worker.alive && (now - worker.last_heartbeat > 6)) { 
                worker.alive = false; // Đánh dấu Worker = FAILED [cite: 161]
                std::cout << "\n[ALERT] Worker " << worker.worker_id << " da bi TIMEOUT (Qua 6s khoang co Heartbeat)!\n";
                
                // CƠ CHẾ PHỤC HỒI LỖI (FAILURE RECOVERY) [cite: 44, 162]
                // Quét hàng đợi tìm các Task đang giao cho Worker bị chết này để chuyển từ RUNNING -> READY [cite: 164, 165, 166, 170]
                for (auto& task : task_queue) {
                    if (task.assigned_worker == worker.worker_id && task.status == "RUNNING") {
                        task.status = "READY"; // Reset về trạng thái sẵn sàng để bộ lập lịch giao cho thợ khác [cite: 170, 171]
                        task.assigned_worker = -1;
                        std::cout << "[RECOVERY] Thu hoi Task " << task.task_id << " tu Worker " 
                                  << worker.worker_id << " ve hang doi READY.\n";
                    }
                }
                worker.current_load = 0;
            }
        }
    }
}

// ============================================================================
// THREAD 2: LUỒNG LẬP LỊCH PHÂN PHỐI TÁC VỤ (SCHEDULER - THUẬT TOÁN FIFO)
// ============================================================================
// Hàm phụ trợ tìm kiếm một Worker còn sống và đang rảnh việc (FIFO/Nhàn rỗi) [cite: 79, 80, 178]
int select_worker_fifo() {
    for (auto& worker : worker_table) {
        if (worker.alive && worker.current_load == 0) { 
            return worker.worker_id; // Chọn Worker này [cite: 19]
        }
    }
    return -1; // Không có Worker nào trống lịch
}

void task_scheduler() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Quét hàng đợi mỗi 0.5 giây [cite: 196]
        
        std::lock_guard<std::mutex> t_lock(task_mutex);
        std::lock_guard<std::mutex> w_lock(worker_mutex);
        
        for (auto& task : task_queue) {
            // Nếu phát hiện có Tác vụ đang ở trạng thái READY [cite: 185]
            if (task.status == "READY") {
                int target_worker_id = select_worker_fifo(); // Lập lịch chọn Worker [cite: 41, 78]
                
                if (target_worker_id != -1) {
                    // Cập nhật trạng thái tác vụ trong nội bộ Master [cite: 42, 187]
                    task.status = "RUNNING";
                    task.assigned_worker = target_worker_id;
                    
                    // Tìm socket kết nối tương ứng của Worker được chọn để gửi thông điệp [cite: 20]
                    for (auto& worker : worker_table) {
                        if (worker.worker_id == target_worker_id) {
                            worker.current_load++; // Tăng tải của Worker [cite: 178]
                            
                            // Đóng gói thông điệp giao việc gửi qua TCP Socket [cite: 100, 109]
                            Message msg;
                            msg.type = TASK_ASSIGN;
                            msg.task_id = task.task_id;
                            msg.input = task.input_data;
                            std::strncpy(msg.operation, task.task_type.c_str(), sizeof(msg.operation));
                            
                            send(worker.socket_fd, &msg, sizeof(Message), 0);
                            std::cout << "[SCHEDULER] Giao Task " << task.task_id 
                                      << " (" << task.task_type << ", Input: " << task.input_data 
                                      << ") cho Worker " << target_worker_id << "\n";
                            break;
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// LUỒNG PHỤ TRỢ: GIAO TIẾP RIÊNG BIỆT VỚI TỪNG WORKER QUA SOCKET
// ============================================================================
void handle_worker_connection(int client_fd) {
    int assigned_id = -1;
    Message msg;
    
    while (true) {
        int valread = recv(client_fd, &msg, sizeof(Message), 0);
        
        // Nếu Worker ngắt kết nối socket đột ngột hoặc có lỗi mạng
        if (valread <= 0) {
            if (assigned_id != -1) {
                std::lock_guard<std::mutex> w_lock(worker_mutex);
                std::lock_guard<std::mutex> t_lock(task_mutex);
                for (auto& worker : worker_table) {
                    if (worker.worker_id == assigned_id) {
                        worker.alive = false;
                        std::cout << "\n[DISCONNECT] Socket cua Worker " << assigned_id << " da bi đóng đột ngột!\n";
                        
                        // Thu hồi khẩn cấp các task của worker này nếu có
                        for (auto& task : task_queue) {
                            if (task.assigned_worker == assigned_id && task.status == "RUNNING") {
                                task.status = "READY";
                                task.assigned_worker = -1;
                            }
                        }
                        worker.current_load = 0;
                    }
                }
            }
            close(client_fd);
            break;
        }

        // Xử lý thông điệp ĐĂNG KÝ (REGISTER) [cite: 56, 104]
        if (msg.type == REGISTER) {
            std::lock_guard<std::mutex> lock(worker_mutex);
            assigned_id = msg.worker_id;
            
            bool exists = false;
            for (auto& worker : worker_table) {
                if (worker.worker_id == assigned_id) {
                    worker.alive = true;
                    worker.last_heartbeat = time(nullptr);
                    worker.socket_fd = client_fd; // Cập nhật lại socket mới nếu kết nối lại
                    exists = true;
                    break;
                }
            }
            
            if (!exists) {
                WorkerInfo new_worker;
                new_worker.worker_id = assigned_id;
                new_worker.alive = true;
                new_worker.current_load = 0;
                new_worker.last_heartbeat = time(nullptr);
                new_worker.socket_fd = client_fd;
                worker_table.push_back(new_worker);
            }
            std::cout << "[MASTER] Da dang ky thanh cong Worker ID: " << assigned_id << " (Status: Alive)\n"; // [cite: 61]
        } 
        // Xử lý thông điệp NHẬN HEARTBEAT [cite: 51, 123, 151]
        else if (msg.type == HEARTBEAT) {
            std::lock_guard<std::mutex> lock(worker_mutex);
            for (auto& worker : worker_table) {
                if (worker.worker_id == msg.worker_id && worker.alive) {
                    worker.last_heartbeat = time(nullptr); // Cập nhật thời gian nhận heartbeat cuối [cite: 156]
                    std::cout << "[HEARTBEAT] Nhan tu Worker ID: " << msg.worker_id << std::endl;
                }
            }
        }
        // Xử lý thông điệp NHẬN KẾT QUẢ TÁC VỤ (TASK_RESULT) [cite: 50, 117]
        else if (msg.type == TASK_RESULT) {
            std::lock_guard<std::mutex> t_lock(task_mutex);
            std::lock_guard<std::mutex> w_lock(worker_mutex);
            
            // Cập nhật trạng thái Task thành COMPLETED [cite: 188]
            for (auto& task : task_queue) {
                if (task.task_id == msg.task_id) {
                    task.status = "COMPLETED";
                    std::cout << "\n[RESULT] Task " << task.task_id << " hoan thanh! Ket qua tu Worker " 
                              << msg.worker_id << " la: " << msg.output << "\n";
                    break;
                }
            }
            
            // Giải phóng tải (current_load) cho Worker sau khi làm xong việc [cite: 178]
            for (auto& worker : worker_table) {
                if (worker.worker_id == msg.worker_id) {
                    if (worker.current_load > 0) worker.current_load--;
                    break;
                }
            }
        }
    }
}

// ============================================================================
// THREAD 1: LUỒNG CHÍNH - KHỞI TẠO SERVER & CHẤP NHẬN KẾT NỐI (ACCEPT CONNECTIONS)
// ============================================================================
int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Khong the tao Socket Server!\n";
        return -1;
    }

    // Cấu hình tái sử dụng cổng cổng tránh lỗi "Address already in use"
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080); // Lắng nghe tại cổng 8080

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Bind cong 8080 that bai!\n";
        return -1;
    }

    if (listen(server_fd, 10) < 0) {
        std::cerr << "Listen that bai!\n";
        return -1;
    }

    std::cout << "[MASTER] He thong khoi dong. Dang lang nghe ket noi tai cong 8080...\n"; // [cite: 194]

    // Nạp sẵn danh sách một vài bài toán tính toán mẫu vào hàng đợi (Task Submission) [cite: 65, 66]
    // Ví dụ: tính giai thừa hoặc đếm số nguyên tố với dữ liệu đầu vào [cite: 71, 72, 73, 130, 131]
    task_queue.push_back({10, "factorial", 12, "READY", -1});
    task_queue.push_back({11, "prime", 100000, "READY", -1});
    task_queue.push_back({12, "factorial", 15, "READY", -1});
    task_queue.push_back({13, "prime", 50000, "READY", -1});
    std::cout << "[MASTER] Da nap san 4 Tác vu mau vao hang doi.\n";

    // Kích hoạt Luồng 2: Bộ lập lịch tác vụ (Scheduler Thread) [cite: 195, 196]
    std::thread scheduler_thread(task_scheduler);
    scheduler_thread.detach();

    // Kích hoạt Luồng 3: Giám sát Heartbeat và lỗi kết nối (Monitor Thread) [cite: 197, 198]
    std::thread monitor_thread(heartbeat_monitor);
    monitor_thread.detach();

    // Vòng lặp liên tục chờ chấp nhận các Worker mới kết nối vào hệ thống [cite: 194]
    while (true) {
        int addrlen = sizeof(address);
        int client_fd = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen);
        if (client_fd < 0) {
            continue;
        }
        
        // Khi một Worker kết nối thành công, tách riêng một luồng con phụ trợ để xử lý truyền thông
        std::thread conn_thread(handle_worker_connection, client_fd);
        conn_thread.detach();
    }

    close(server_fd);
    return 0;
}
