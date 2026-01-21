// subscriber/src/subscriber.cpp
#include <iostream>
#include <signal.h>
#include <atomic>
#include <chrono>
#include <sw/redis++/redis++.h>
#include <iomanip> // 为了 std::put_time
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

        // 创建 Redis 连接
        std::stringstream conn_info;
        conn_info << "tcp://" << redis_host << ":" << redis_port;
        if (!redis_password.empty())
        {
            conn_info << "?password=" << redis_password;
        }
        auto redis = sw::redis::Redis(conn_info.str());

        // 创建订阅者
        auto sub = redis.subscriber();

        std::cout << "=== Redis 订阅者 ===" << std::endl;
        std::cout << "已连接到: " << redis_host << ":" << redis_port << std::endl;
        std::cout << "按 Ctrl+C 退出" << std::endl;

        // 订阅频道
        sub.subscribe("chat_room");

        // 接收消息
        while (running)
        {
            try
            {
                // 设置消息回调
                sub.on_message([](std::string channel, std::string msg)
                {
                    auto now_time = std::time(nullptr);
                    std::cout << "[" << std::put_time(std::gmtime(&now_time), "%H:%M:%S") << "] ";
                    std::cout << "频道: " << channel << ", 消息: " << msg << std::endl; 
                });

                // 订阅频道
                sub.subscribe("chat_room");
                std::cout << "开始订阅频道: chat_room" << std::endl;

                // 开始消费消息（阻塞）
                sub.consume();
            }
            catch (const sw::redis::Error &e)
            {
                std::cerr << "Redis错误: " << e.what() << std::endl;
                // 可选：短暂延迟后重试
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            catch (const std::exception &e)
            {
                std::cerr << "接收消息错误: " << e.what() << std::endl;
                break;
            }
        }
        sub.unsubscribe("chat_room");
        std::cout << "\n已取消订阅频道: " << "chat_room" << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "错误: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}