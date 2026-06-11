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

// Bảng quản lý nội bộ của Master
std::vector<WorkerInfo> worker_table; 
std::vector<Task> task_queue;         

// Mutex đồng bộ hóa tránh xung đột luồng
std::mutex worker_mutex;
std::mutex task_mutex;

// Cấu hình thuật toán lập lịch: 0 = FIFO, 1 = Round Robin, 2 = Least Loaded
int scheduling_policy = 0; 
int rr_index = 0; 

// Biến phục vụ luồng tự động bấm giờ thí nghiệm
std::chrono::steady_clock::time_point start_time;
bool is_experiment_started = false;
int total_tasks_inserted = 0;

// ============================================================================
// HÀM CHỌN WORKER LINH HOẠT THEO THUẬT TOÁN ĐÃ CHỌN
// ============================================================================
int select_worker() {
    if (worker_table.empty()) return -1;

    // --- 1. THUẬT TOÁN FIFO (Chọn worker rảnh đầu tiên) ---
    if (scheduling_policy == 0) {
        for (auto& worker : worker_table) {
            if (worker.alive && worker.current_load == 0) {
                return worker.worker_id;
            }
        }
    }
    // --- 2. THUẬT TOÁN ROUND ROBIN (Luân phiên xoay vòng) ---
    else if (scheduling_policy == 1) {
        int total_workers = worker_table.size();
        for (int i = 0; i < total_workers; i++) {
            int idx = (rr_index + i) % total_workers;
            if (worker_table[idx].alive) {
                rr_index = (idx + 1) % total_workers;
                return worker_table[idx].worker_id;
            }
        }
    }
    // --- 3. THUẬT TOÁN LEAST LOADED (Giao cho thằng ít việc nhất) ---
    else if (scheduling_policy == 2) {
        int min_load = 999999;
        int target_id = -1;
        for (auto& worker : worker_table) {
            if (worker.alive && worker.current_load < min_load) {
                min_load = worker.current_load;
                target_id = worker.worker_id;
            }
        }
        return target_id;
    }
    return -1;
}

// ============================================================================
// THREAD 3: GIÁM SÁT TIMEOUT HEARTBEAT (FAULT TOLERANCE)
// ============================================================================
void heartbeat_monitor() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1)); 
        
        std::lock_guard<std::mutex> w_lock(worker_mutex);
        std::lock_guard<std::mutex> t_lock(task_mutex);
        
        time_t now = time(nullptr);
        for (auto& worker : worker_table) {
            if (worker.alive && (now - worker.last_heartbeat > 6)) { 
                worker.alive = false; 
                std::cout << "\n[ALERT] Worker " << worker.worker_id << " bi TIMEOUT (Qua 6s khong co Heartbeat)!\n";
                
                // Thu hồi tác vụ dở dang về trạng thái READY để phân phối lại
                for (auto& task : task_queue) {
                    if (task.assigned_worker == worker.worker_id && task.status == "RUNNING") {
                        task.status = "READY"; 
                        task.assigned_worker = -1;
                        std::cout << "[RECOVERY] Thu hoi Task " << task.task_id << " ve hang doi READY.\n";
                    }
                }
                worker.current_load = 0;
            }
        }
    }
}

// ============================================================================
// THREAD 2: LUỒNG LẬP LỊCH PHÂN PHỐI TÁC VỤ (SCHEDULER THREAD)
// ============================================================================
void task_scheduler() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
        
        std::lock_guard<std::mutex> t_lock(task_mutex);
        std::lock_guard<std::mutex> w_lock(worker_mutex);
        
        for (auto& task : task_queue) {
            if (task.status == "READY") {
                int target_worker_id = select_worker(); 
                
                if (target_worker_id != -1) {
                    // Kích hoạt đồng hồ bấm giờ khi tác vụ ĐẦU TIÊN được phân phối
                    if (!is_experiment_started) {
                        start_time = std::chrono::steady_clock::now();
                        is_experiment_started = true;
                        std::cout << "\n[TIMER] Bat dau tinh thoi gian xu ly cho toan bo hang doi...\n";
                    }

                    task.status = "RUNNING";
                    task.assigned_worker = target_worker_id;
                    
                    for (auto& worker : worker_table) {
                        if (worker.worker_id == target_worker_id) {
                            worker.current_load++; 
                            
                            Message msg;
                            msg.type = TASK_ASSIGN;
                            msg.task_id = task.task_id;
                            msg.input = task.input_data;
                            std::strncpy(msg.operation, task.task_type.c_str(), sizeof(msg.operation));
                            
                            send(worker.socket_fd, &msg, sizeof(Message), 0);
                            std::cout << "[SCHEDULER] Giao Task " << task.task_id 
                                      << " (" << task.task_type << ") cho Worker " << target_worker_id << "\n";
                            break;
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// LUỒNG XỬ LÝ TRUYỀN THÔNG VỚI TỪNG WORKER SOCKET
// ============================================================================
void handle_worker_connection(int client_fd) {
    int assigned_id = -1;
    Message msg;
    
    while (true) {
        int valread = recv(client_fd, &msg, sizeof(Message), 0);
        
        // Phát hiện Worker ngắt kết nối đột ngột (Ctrl+C)
        if (valread <= 0) {
            if (assigned_id != -1) {
                std::lock_guard<std::mutex> w_lock(worker_mutex);
                std::lock_guard<std::mutex> t_lock(task_mutex);
                for (auto& worker : worker_table) {
                    if (worker.worker_id == assigned_id) {
                        worker.alive = false;
                        std::cout << "\n[DISCONNECT] Socket cua Worker " << assigned_id << " da bi dong dot ngot!\n";
                        
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

        // Xử lý gói ĐĂNG KÝ
        if (msg.type == REGISTER) {
            std::lock_guard<std::mutex> lock(worker_mutex);
            assigned_id = msg.worker_id;
            
            bool exists = false;
            for (auto& worker : worker_table) {
                if (worker.worker_id == assigned_id) {
                    worker.alive = true;
                    worker.last_heartbeat = time(nullptr);
                    worker.socket_fd = client_fd; 
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
            std::cout << "[MASTER] Da dang ky thanh cong Worker ID: " << assigned_id << " (Status: Alive)\n";
        } 
        // Xử lý gói HEARTBEAT
        else if (msg.type == HEARTBEAT) {
            std::lock_guard<std::mutex> lock(worker_mutex);
            for (auto& worker : worker_table) {
                if (worker.worker_id == msg.worker_id && worker.alive) {
                    worker.last_heartbeat = time(nullptr);
                }
            }
        }
        // Xử lý gói KẾT QUẢ (TASK_RESULT)
        else if (msg.type == TASK_RESULT) {
            std::lock_guard<std::mutex> t_lock(task_mutex);
            std::lock_guard<std::mutex> w_lock(worker_mutex);
            
            int completed_count = 0;
            for (auto& task : task_queue) {
                if (task.task_id == msg.task_id) {
                    task.status = "COMPLETED";
                    std::cout << "[RESULT] Task " << task.task_id << " hoan thanh tren Worker " << msg.worker_id << "\n";
                }
                if (task.status == "COMPLETED") {
                    completed_count++;
                }
            }
            
            for (auto& worker : worker_table) {
                if (worker.worker_id == msg.worker_id) {
                    if (worker.current_load > 0) worker.current_load--;
                    break;
                }
            }

            // KIỂM TRA ĐIỀU KIỆN KẾT THÚC THÍ NGHIỆM ĐỂ XUẤT SỐ LIỆU
            if (completed_count == total_tasks_inserted && is_experiment_started) {
                auto end_time = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
                double duration_seconds = duration / 1000.0;
                
                std::cout << "\n========================================================\n";
                std::cout << "[KET QUA KIEM THU / EXPERIMENT RESULT]\n";
                std::cout << "• Thuat toan lap lich dang dung: ";
                if(scheduling_policy == 0) std::cout << "FIFO\n";
                else if(scheduling_policy == 1) std::cout << "Round Robin\n";
                else std::cout << "Least Loaded\n";
                std::cout << "• Tong so tac vu: " << total_tasks_inserted << " tasks.\n";
                std::cout << "• Thoi gian hoan thanh (Completion Time): " << duration_seconds << " giay.\n";
                std::cout << "• Nang suat (Throughput): " << (total_tasks_inserted / duration_seconds) << " tasks/giay.\n";
                std::cout << "========================================================\n\n";
                
                is_experiment_started = false; 
            }
        }
    }
}

// ============================================================================
// THREAD 1: CHÍNH (SERVER SETUP & ACCEPT KẾT NỐI)
// ============================================================================
int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0 || listen(server_fd, 10) < 0) {
        std::cerr << "Khoi tao Server that bai!\n";
        return -1;
    }

    std::cout << "[MASTER] Dang lang nghe tai cong 8080...\n";

    // CHỌN THUẬT TOÁN TẠI ĐÂY TRƯỚC KHI BUILD: 0 = FIFO, 1 = Round Robin, 2 = Least Loaded
    scheduling_policy = 0; 

    // TỰ ĐỘNG SINH 100 TASKS PHỤC VỤ THÍ NGHIỆM ĐỀ BÀI YÊU CẦU
    int test_tasks = 100; 
    for (int i = 1; i <= test_tasks; i++) {
        if (i % 2 == 0) {
            task_queue.push_back({i, "factorial", 12, "READY", -1}); // Tính 12!
        } else {
            task_queue.push_back({i, "prime", 30000, "READY", -1});   // Đếm SNT đến 30000
        }
    }
    total_tasks_inserted = task_queue.size();
    std::cout << "[MASTER] Da nap san " << total_tasks_inserted << " tac vu vao hang doi.\n";

    std::thread scheduler_thread(task_scheduler);
    scheduler_thread.detach();

    std::thread monitor_thread(heartbeat_monitor);
    monitor_thread.detach();

    while (true) {
        int addrlen = sizeof(address);
        int client_fd = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen);
        if (client_fd >= 0) {
            std::thread conn_thread(handle_worker_connection, client_fd);
            conn_thread.detach();
        }
    }

    close(server_fd);
    return 0;
}
