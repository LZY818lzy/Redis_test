#include <sw/redis++/redis++.h>
#include <iostream>
#include <chrono>
#include <thread>

using namespace sw::redis;

int main() {
    try {
        // 1. 连接 Redis——使用 ConnectionOptions
        ConnectionOptions opts;
        opts.host = "140.32.1.192";
        opts.port = 6379;
        opts.password = "ggl2e=mc2";
        auto redis = Redis(opts);
        std::cout << "成功连接到 Redis" << std::endl;

        // 测试连接
        redis.ping();
        std::cout << "服务器连接正常" << std::endl;
        
        // 2. SET/GET 操作
        redis.set("mykey", "Hello Redis++");
        auto value = redis.get("mykey");
        if (value) {
            std::cout << "GET mykey: " << *value << std::endl;
        }
        std::cout << "GET name:" << *redis.get("name") << std::endl;
        
        // // 3. 简单的发布订阅演示
        // std::cout << "\n开始发布订阅演示..." << std::endl;
        
        // // 创建订阅者
        // auto sub = redis.subscriber();
        
        // // 在单独的线程中运行订阅者
        // std::thread([&sub]() {
        //     sub.on_message([](std::string channel, std::string msg) {
        //         std::cout << "[订阅者] 频道: " << channel 
        //                   << " 消息: " << msg << std::endl;
        //     });
        //     sub.subscribe("test_channel");
        //     sub.consume();  // 这会阻塞
        // }).detach();
        
        // // 等待订阅者就绪
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // // 发布消息
        // redis.publish("test_channel", "第一条测试消息");
        // redis.publish("test_channel", "第二条测试消息");
        
        // // 等待消息被处理
        // std::this_thread::sleep_for(std::chrono::seconds(1));
        
        // std::cout << "演示完成" << std::endl;
        
    } catch (const Error &e) {
        std::cerr << "Redis 错误: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}