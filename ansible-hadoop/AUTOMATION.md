# Автоматизация

Сделана с помощью ansible.

![](./src/cluster.svg)

## Запуск

Обновите параметры подключения к jump-ноде в инвенторке:

```bash
cd inventory
vim hosts.yml
```

Находясь в той же директории, создайте вольты с паролем от пользователя
`ubuntu`:

```bash
ansible-vault create ./group_vars/nodes/vault.yml
# ansible_ssh_pass: "PASSWORD"
ansible-vault create ./group_vars/all/vault.yml
# ansible_become_pass: "PASSWORD"
```

Теперь можно запускать плейбук, который настроит ВМ и установит на них
hadoop:

```bash
ansible-playbook -i hosts.yml --ask-vault-pass ../ansible/playbooks/hadoop.yml
```

## Доступ к веб-интерфейсам hadoop

Необходимые сервисы расположены на портах 9870, 8088 и 19888 нейм-ноды, для
доступа вам нужно подключиться к jump-ноде с помощью консольной команды:

```bash
ssh \
    -L 9870:192.168.10.25:9870 \
    -L 8088:192.168.10.25:8088 \
    -L 19888:192.168.10.25:19888 \
    ubuntu@178.236.25.103
```

Пока туннель активен, эндпоинты кластера будут доступны по ссылам:

- **HDFS** -- [http://localhost:9870](http://localhost:9870)
- **Resource Manager** -- [http://localhost:8088](http://localhost:8088)
- **JobHistoryServer** -- [http://localhost:19888](http://localhost:19888)

Однако для удобства можете настроить ssh-конфиг.

### Настройка ssh-конфига

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

