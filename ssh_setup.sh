#!/bin/bash

# 创建密码
echo "请创建 root 用户密码："
passwd

# 替换国内源
sed -i.bak 's|http://archive.ubuntu.com|https://mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list
sed -i.bak 's|http://security.ubuntu.com|https://mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list

# 更新软件包并安装 SSH 服务
apt update && apt install -y openssh-server

# 修改 SSH 配置
sed -i 's/^#\(PubkeyAuthentication yes\)/\1/' /etc/ssh/sshd_config
sed -i 's/^#\(PermitRootLogin\) prohibit-password/\1 yes/' /etc/ssh/sshd_config
# 取消注释并修改端口号
# sed -i 's/^#Port 22/Port 10022/' /etc/ssh/sshd_config

# 重启 SSH 服务
service ssh restart

# 设置免密登录
echo "请将以下密钥添加到远程容器的 ~/.ssh/authorized_keys 文件中："
mkdir -p ~/.ssh
touch ~/.ssh/authorized_keys

echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCsCg52JcuUIqo8oGGSmrOZfq9Oafs4QrmU4OFsP+6AGHvuwrnqQnzNjtnYOsdtVOCLxqtgktiWUKk2+2Z/YWfQj+mwGGXSpUrVW6aByKtCoAQwPDwPECOFTD5vjeyZ4fao5ltVEcghOkZ92Zh/LDkPZwspHr/5YP1H6akbjgKPakG4IVXGSkxRpyQfDmh5E0PmQM4DimYw6RToQ/5ldimxlNGdawm0ZbhQ4wSce/tUaBWs1rl/jRUONhWnyRyZa7hIPPaYj883PaaF8Rcs27NdbSTttRrFecRzR9BbwkSHhzxduikkb9uQfQg7qFsIcRWrcx64SQ1AuMf3vWqIKp4ikK6ysUkCZk44ibn3s5vrq8zBdCyPOxmudThUg1NwIPgVZ5NnuBJcfx+uIlXklAJ7edlzSnMWsjLOjM6y7Uv7Y1yrBk21kr2N+PlfnO1WqLt9eOvgf1u1ttO7Qxsf2fEmRaDjqiByHWnzrVlGYnbLhSJvSKMlaMm+r8RAstH77pevl8ISYwxqhVFaFtUoaGDW+sIHjjsO4UvjfH5dQCwNvRX2lWDW1BUjbR3ZtuCnuEzVBOzG4FKqFoA2pdXQIPlSh1rKcYFkuDN3P3GH8wbva+PJyBeYozxcEULF9XT7toZBJbF0wOcl+roNIAqIWv6gzm0x65CEIZYzQZyEBkVGww== qpdongjie@gmail.com" >> ~/.ssh/authorized_keys

echo "密钥已添加到 ~/.ssh/authorized_keys 文件中。"

# 重启 SSH 服务以应用更改
service ssh restart

echo "脚本执行完毕。"

