# Автоматизация

Сделана с помощью ansible.

![](./src/cluster.svg)

## Запуск

Обновите параметры подключения к jump-ноде в инвенторке:

```bash
cd inventory
vim hosts.yml
```

Также, находясь в той же директории, создайте вольты с паролем от
пользователя `ubuntu`:

```bash
ansible-vault create ./group_vars/nodes/vault.yml
# ansible_ssh_pass: "PASSWORD"
ansible-vault create ./group_vars/all/vault.yml
# ansible_become_pass: "PASSWORD"
```

Теперь можно запускать плейбук, который настроит ВМ и установит на них
hdfs-кластер:

```bash
ansible-playbook -i hosts.yml --ask-vault-pass ../ansible/playbooks/hadoop.yml
```

## Доступ к веб-интерфейсам hadoop

Необходимые сервисы расположены на портах 9870, 8088 и 19888 нейм-ноды. Вы можете получить к ним доступ, подключившись к jump-ноде с помощью консольной команды:

```bash
ssh \
    -L 9870:192.168.10.25:9870 \
    -L 8088:192.168.10.25:8088 \
    -L 19888:192.168.10.25:19888 \
    ubuntu@178.236.25.103
```

Однако для удобства можете настроить ssh-конфиг.

### Настройка ssh-конфига

Автоматика использует ваш ключ только для подключения к jump-ноде, далее она
создаёт cluster-ключ, который разносит на все ВМ на стенде. Пользователю этот
ключ не передаётся, поэтому, для получения доступа к веб-интерфейсам у вас есть
два пути, которые будут описаны далее.

**Первый вариант** -- настроить ssh-config для подключения на нейм-ноду через
джамп:

```config
Host big-data-jn
  HostName 178.236.25.103 
  User ubuntu
  IdentityFile ~/.ssh/id_rsa

Host big-data-nn
  HostName 192.168.10.25
  User ubuntu
  IdentityFile ~/.ssh/id_rsa
  LocalForward 9870 localhost:9870
  LocalForward 8088 localhost:8088
  LocalForward 19888 localhost:19888
  ProxyJump big-data-jn
```

Для этого будет необходимо внести в authorized_keys нейм-ноды ваш личный ключ.

Далее -- `ssh big-data-nn`.

**Второй вариант** -- как на вебинаре, подключаться к jump-ноде и прокидывать
себе порты с интерфейса нейм-ноды:

```config
Host big-data-jn
  HostName 178.236.25.103 
  User ubuntu
  IdentityFile ~/.ssh/id_rsa
  LocalForward 9870 192.168.10.25:9870
  LocalForward 8088 192.168.10.25:8088
  LocalForward 19888 192.168.10.25:19888
```

Далее -- `ssh big-data-jn`.

