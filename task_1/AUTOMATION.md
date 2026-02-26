Для запуска автоматизации обновите параметры подключения к jump-ноде в
инвенторке:

```bash
cd inventory
vim hosts.yml
```

А также, находясь в той же директории, создайте вольты с паролем от
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

