#pragma once

#include <string>
// #include <iostream>  // 添加这一行，用于 std::cerr
#include "yaml-cpp/yaml.h"

class CConfig
{

public:
    // 1、获取单例实例
    static CConfig &GetInstance()
    {
        // 这种实现方式：使用了C++11的静态局部变量特性
        static CConfig instance; // C++11 保证线程安全
        return instance;
    }
    // 2、加载配置文件
    bool Load(const std::string &confName)
    {
        try
        {
            config = YAML::LoadFile(confName);
            return true;
        }
        catch (const YAML::Exception &e)
        {
            // 不再依赖 iostream，错误处理留给调用者
            last_error_ = e.what();
            return false;
        }
    }
    // 3、获取最后的错误信息
    std::string GetLastError() const { return last_error_; }

    // 4、获取配置
    //  获取字符串
    std::string GetString(const std::string &itemname) // （无默认值）
    {

        return config[itemname].as<std::string>();
    }
    std::string GetStringDefault(const std::string &itemname, const std::string &default_value) // （有默认值）
    {
        if (config[itemname])
        {
            return config[itemname].as<std::string>();
        }
        return default_value;
    }
    // 获取整型
    int GetInt(const std::string &itemname)
    {
        return config[itemname].as<int>();
    }
    int GetIntDefault(const std::string &itemname, int default_value)
    {
        if (config[itemname])
        {
            return config[itemname].as<int>();
        }
        return default_value;
    }
    // 获取双精度浮点型
    double GetDouble(const std::string &itemname)
    {

        return config[itemname].as<double>();
    }
    double GetDoubleDefault(const std::string &itemname, double default_value)
    {
        if (config[itemname])
        {
            return config[itemname].as<double>();
        }
        return default_value;
    }
    // 获取布尔型
    bool GetBool(const std::string &itemname)
    {
        return config[itemname].as<bool>();
    }
    bool GetBoolDefault(const std::string &itemname, bool default_value)
    {
        if (config[itemname])
        {
            return config[itemname].as<bool>();
        }
        return default_value;
    }

    // 5、防止拷贝
    CConfig(const CConfig &) = delete;
    CConfig &operator=(const CConfig &) = delete;

private:
    // 成员变量
    CConfig() = default;  // 私有构造函数
    ~CConfig() = default; // 私有析构函数

    YAML::Node config;       // YAML 配置节点
    std::string last_error_; // 存储最后的错误信息
};