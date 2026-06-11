cmake_minimum_required(VERSION 3.10)
project(DistributedTaskScheduler)

# Cấu hình C++11 trở lên (để dùng đa luồng sau này)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Tìm thư viện Threads (pthread) trên Linux
find_package(Threads REQUIRED)

# Đảm bảo compiler tìm thấy file trong thư mục common
include_directories(common)

# Cấu hình biên dịch cho Master
add_executable(master master_src/master.cpp)
target_link_libraries(master PRIVATE Threads::Threads)

# Cấu hình biên dịch cho Worker
add_executable(worker worker_src/worker.cpp)
target_link_libraries(worker PRIVATE Threads::Threads)
