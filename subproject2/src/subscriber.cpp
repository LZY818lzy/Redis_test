// subscriber/src/subscriber.cpp
#include <iostream>// 标准输入输出流库，用于控制台输入输出（cout/cin/cerr）
#include <signal.h>// 信号处理库，用于处理 Ctrl+C（SIGINT）等信号
#include <atomic>// 原子操作库，用于线程安全的布尔标志（running变量）
#include <chrono>// 时间库，用于时间相关操作（chrono::milliseconds等）
#include <sw/redis++/redis++.h>
#include <thread>  // 多线程支持库，用于sleep_for函数让线程休眠
#include <ctime>   // C时间函数库，提供time_t、localtime等时间处理函数
#include <iomanip> // 输入输出格式控制库，用于格式化输出时间（std::put_time）
#include "CConfig.h"


std::atomic<bool> running{true};

void signal_handler(int)
{
    running = false;
}

int main()
{
    signal(SIGINT, signal_handler);  // 捕捉 Ctrl+C 信号
    signal(SIGTERM, signal_handler); // 捕捉kill信号

    try
    {
        // 读取配置
        auto &config = CConfig::GetInstance();
        std::string configPath = "../config/redis_test.yaml";
        if (!config.Load(configPath))
        {
            std::cerr << "警告: " << config.GetLastError() << std::endl;
            std::cerr << "将使用默认配置运行" << std::endl;
        }

        // 获取 Redis 连接参数
        std::string redis_host = config.GetStringDefault("host", "140.32.1.192");
        int redis_port = config.GetIntDefault("port", 6379);
        std::string redis_password = config.GetStringDefault("password", "ggl2e=mc2");

        // 使用 ConnectionOptions 创建 Redis 连接
        // 关键：设置 socket_timeout 为非0值（如1000ms），这样 consume() 才会有超时
        sw::redis::ConnectionOptions conn_opts;
        conn_opts.host = redis_host;         // 主机地址
        conn_opts.port = redis_port;         // 端口号
        conn_opts.password = redis_password; // 密码（包含特殊字符也没问题）
        conn_opts.socket_timeout = std::chrono::milliseconds(1000); // 设置 socket 超时，确保 consume() 有超时

        auto redis = sw::redis::Redis(conn_opts);

        // 创建订阅者
        auto sub = redis.subscriber();

        std::cout << "=== Redis 订阅者 ===" << std::endl;
        std::cout << "已连接到: " << redis_host << ":" << redis_port << std::endl;
        std::cout << "按 Ctrl+C 退出" << std::endl;

        // 设置消息回调
        sub.on_message([](std::string channel, std::string msg)
        {
            auto now = std::chrono::system_clock::now();
            std::time_t now_time = std::chrono::system_clock::to_time_t(now);
            std::cout << "[" << std::put_time(std::localtime(&now_time), "%H:%M:%S") << "] ";
            std::cout << "频道: " << channel << ", 消息: " << msg << std::endl;
        });

        // 订阅频道
        sub.subscribe("chat_room");

        // 接收消息
        while (running)
        {
            try
            {
                // consume() 会阻塞直到：
                // 1. 收到消息（触发回调）
                // 2. socket_timeout 超时（抛出 TimeoutError）
                // 3. 发生其他错误
                sub.consume();
            }
            catch (const sw::redis::TimeoutError &)  // 处理超时
            {
                // 超时是正常的，继续循环检查 running 状态
                // 这样 Ctrl+C 设置 running=false 时，能快速退出
                continue;
            }
            catch (const sw::redis::Error &e)
            {
                if (!running) {
                    break; // 正常退出
                }
                std::cerr << "Redis错误: " << e.what() << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            catch (const std::exception &e)
            {
                if (!running) {
                    break; // 正常退出
                }
                std::cerr << "接收消息错误: " << e.what() << std::endl;
                break;
            }
        }
        sub.unsubscribe("chat_room");// 取消订阅
        std::cout << "\n已取消订阅频道: " << "chat_room" << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "错误: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}