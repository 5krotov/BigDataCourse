```bash
cd inventory
vim hosts.yml
# обновите параметры подключения к jump-ноде и использования её как прокси-jump

ansible-vault create ./group_vars/nodes/vault.yml
# ansible_ssh_pass: "PASSWORD"
```

```bash
cd inventory
ansible-playbook -i hosts.yml --ask-vault-pass ../ansible/playbooks/hadoop.yml
```
