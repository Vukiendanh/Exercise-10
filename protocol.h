#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <ctime>
#include <string>

enum MessageType {
    REGISTER,
    HEARTBEAT,
    TASK_ASSIGN, // Master giao việc cho Worker
    TASK_RESULT  // Worker tra ket qua cho Master
};

struct Message {
    MessageType type;
    int worker_id;
    int task_id;
    char operation[32]; // "prime", "factorial", v.v.
    long long input;    // Du lieu dau vao (gia su dung so nguyen cho don gian)
    long long output;   // Ket qua tra ve
};

struct WorkerInfo {
    int worker_id;
    bool alive;
    int current_load; // Luu so task dang chay de dung cho thuat toan Least Loaded
    time_t last_heartbeat;
    int socket_fd;
};

struct Task {
    int task_id;
    std::string task_type;
    long long input_data;
    std::string status; // "READY", "RUNNING", "COMPLETED"
    int assigned_worker;
};

#endif
