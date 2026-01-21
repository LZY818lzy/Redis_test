// publisher/src/publisher.cpp

#include <iostream>
#include <string>
#include <sw/redis++/redis++.h>
#include "CConfig.h"

int main()
{
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
        sw::redis::ConnectionOptions conn_opts;
        conn_opts.host = redis_host;         // 主机地址
        conn_opts.port = redis_port;         // 端口号
        conn_opts.password = redis_password; // 密码（包含特殊字符也没问题）
        auto redis = sw::redis::Redis(conn_opts);

        std::cout << "=== Redis 发布者 ===" << std::endl;
        std::cout << "已连接到: " << redis_host << ":" << redis_port << std::endl;
        std::cout << "\n使用方法:" << std::endl;
        std::cout << "  1. 输入频道名称" << std::endl;
        std::cout << "  2. 输入消息内容" << std::endl;
        std::cout << "  3. 按回车发送" << std::endl;
        std::cout << "  输入 'quit' 退出" << std::endl;
        std::cout << "\n开始发布消息..." << std::endl;

        std::string channel, message;

        // 主循环：读取用户输入并发布消息
        while (true)
        {
            std::cout << "\n频道: ";
            std::getline(std::cin, channel); // 从标准输入读取频道名称（包括空格）

            if (channel == "quit" || channel == "exit")
            {
                break;
            }

            std::cout << "消息: ";
            std::getline(std::cin, message);

            if (message == "quit" || message == "exit")
            {
                break;
            }

            // 发布消息（核心功能）
            int receivers = redis.publish(channel, message);

            std::cout << "✓ 已发布到频道 \"" << channel
                      << "\"，接收者数量: " << receivers << std::endl;
            // 根据接收者数量给出不同提示
            if (receivers == 0)
            {
                std::cout << "⚠️  提示：当前没有订阅者接收此消息。" << std::endl;
                std::cout << "   请检查：" << std::endl;
                std::cout << "   1. 频道名是否正确（无多余空格）" << std::endl;
                std::cout << "   2. 是否有订阅者连接到该频道" << std::endl;
                std::cout << "   3. 订阅者程序是否正常运行" << std::endl;
            }
        }
        std::cout << "\n发布者已退出" << std::endl;
    }
        catch (const std::exception &e)
        {
            std::cerr << "错误: " << e.what() << std::endl;
            return 1;
        }

        return 0;
    }

