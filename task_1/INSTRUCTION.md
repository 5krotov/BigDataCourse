```
PUBKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOhXVs7LG89sMQOU5Ahame9XgfqqVG8Y1EMdnrsJfaww hadoop@team-06-jn"
UBUNTU_PASS="CfI16x2O"

# Создать пользователя
echo "$UBUNTU_PASS" | sudo -S useradd -m -s /bin/bash hadoop 2>/dev/null || echo "user already exists"

# Создать .ssh и положить ключ
echo "$UBUNTU_PASS" | sudo -S mkdir -p /home/hadoop/.ssh
echo "$UBUNTU_PASS" | sudo -S bash -c "echo '$PUBKEY' > /home/hadoop/.ssh/authorized_keys"
echo "$UBUNTU_PASS" | sudo -S chmod 700 /home/hadoop/.ssh
echo "$UBUNTU_PASS" | sudo -S chmod 600 /home/hadoop/.ssh/authorized_keys
echo "$UBUNTU_PASS" | sudo -S chown -R hadoop:hadoop /home/hadoop/.ssh

# Проверить результат
echo "=== authorized_keys ==="
sudo cat /home/hadoop/.ssh/authorized_keys
echo "=== permissions ==="
sudo ls -la /home/hadoop/.ssh/
echo "=== user info ==="
id hadoop

```


с jn node из под hadoop
```
PRIVKEY=$(cat /home/hadoop/.ssh/id_ed25519)
PUBKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOhXVs7LG89sMQOU5Ahame9XgfqqVG8Y1EMdnrsJfaww hadoop@team-06-jn"
ALL_NODES="192.168.10.53 192.168.10.25 192.168.10.23 192.168.10.24"
REMOTE_NODES="192.168.10.25 192.168.10.23 192.168.10.24"

for NODE in $REMOTE_NODES; do
  echo "=== $NODE ==="
  ssh -o StrictHostKeyChecking=no hadoop@$NODE <<EOF
# Приватный ключ
cat > ~/.ssh/id_ed25519 <<'KEYEOF'
$PRIVKEY
KEYEOF

# Публичный ключ
echo "$PUBKEY" > ~/.ssh/id_ed25519.pub

# known_hosts — чтобы не было интерактивных запросов
ssh-keyscan -H $ALL_NODES 2>/dev/null > ~/.ssh/known_hosts

# Права
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub
chmod 600 ~/.ssh/known_hosts

echo "Done on \$(hostname)"
ls -la ~/.ssh/
EOF
done

echo "Done on $(hostname)"
```

Install hadoop
```

cd ~
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

# Распаковать
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop


for NODE in 192.168.10.25 192.168.10.23 192.168.10.24; do
  echo "=== $NODE ==="
  scp hadoop-3.3.6.tar.gz hadoop@$NODE:~
  ssh hadoop@$NODE "tar -xzf ~/hadoop-3.3.6.tar.gz && mv ~/hadoop-3.3.6 ~/hadoop"
done

```


но не хватило прав на dn-00 и dn-01
```
# Под ubuntu на jn
UBUNTU_PASS="CfI16x2O"

for NODE in 192.168.10.23 192.168.10.24; do
  echo "=== $NODE ==="
  sshpass -p "$UBUNTU_PASS" ssh -o StrictHostKeyChecking=no ubuntu@$NODE \
    "echo '$UBUNTU_PASS' | sudo -S chown -R hadoop:hadoop /home/hadoop"
done
```

Пропишем энвы
```
for NODE in 192.168.10.53 192.168.10.25 192.168.10.23 192.168.10.24; do
  echo "=== $NODE ==="
  ssh hadoop@$NODE bash << 'EOF'
JAVA_PATH=$(readlink -f $(which java) | sed 's|/bin/java||')

grep -q 'HADOOP_HOME' ~/.bash_profile 2>/dev/null || cat >> ~/.bash_profile << ENVBLOCK

# Hadoop env
export JAVA_HOME=$JAVA_PATH
export HADOOP_HOME=$HOME/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin
ENVBLOCK

EOF
done
```

```
for NODE in 192.168.10.53 192.168.10.25 192.168.10.23 192.168.10.24; do
  ssh hadoop@$NODE bash << 'EOF'
JAVA_PATH=$(readlink -f $(which java) | sed 's|/bin/java||')
sed -i "s|# export JAVA_HOME=.*|export JAVA_HOME=$JAVA_PATH|" ~/hadoop/etc/hadoop/hadoop-env.sh
echo "=== $(hostname): $(grep 'export JAVA_HOME' ~/hadoop/etc/hadoop/hadoop-env.sh) ==="
EOF
done

```

```
for NODE in 192.168.10.53 192.168.10.25 192.168.10.23 192.168.10.24; do
  ssh hadoop@$NODE "cat > ~/hadoop/etc/hadoop/core-site.xml" << 'EOF'
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://192.168.10.25:9000</value>
    </property>
</configuration>
EOF
done

```


```
for NODE in 192.168.10.53 192.168.10.25 192.168.10.23 192.168.10.24; do
  ssh hadoop@$NODE bash << 'EOF'
cat > ~/hadoop/etc/hadoop/hdfs-site.xml << 'XML'
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.namenode.rpc-bind-host</name>
        <value>0.0.0.0</value>
    </property>
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>192.168.10.25:9868</value>
    </property>
</configuration>
XML
EOF
done

```

```
for NODE in 192.168.10.53 192.168.10.25 192.168.10.23 192.168.10.24; do
  ssh hadoop@$NODE "cat > ~/hadoop/etc/hadoop/workers" << 'EOF'
192.168.10.23
192.168.10.24
192.168.10.25
EOF
done

```



```
UBUNTU_PASS="CfI16x2O"

HOSTS_BLOCK="
192.168.10.53 team-06-jn
192.168.10.25 team-06-nn
192.168.10.23 team-06-dn-00
192.168.10.24 team-06-dn-01"

for NODE in 192.168.10.25 192.168.10.23 192.168.10.24; do
  echo "=== $NODE ==="
  sshpass -p "$UBUNTU_PASS" ssh ubuntu@$NODE \
    "echo '$UBUNTU_PASS' | sudo -S bash -c \"echo '$HOSTS_BLOCK' >> /etc/hosts\""
done


```
И на jn напрямую
```
echo "$HOSTS_BLOCK" | sudo tee -a /etc/hosts
```



```
ssh hadoop@192.168.10.25 "~/hadoop/bin/hdfs namenode -format"

ssh hadoop@192.168.10.25 "~/hadoop/sbin/start-dfs.sh"

for NODE in 192.168.10.25 192.168.10.23 192.168.10.24; do
  echo "=== $NODE ==="
  ssh hadoop@$NODE "jps"
done

```

вывод
stop
```
ssh hadoop@192.168.10.25 "~/hadoop/sbin/stop-dfs.sh"
```
