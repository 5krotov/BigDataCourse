
```bash
cd inventory
ansible-vault create ./group_vars/nodes/vault.yml
# ansible_ssh_pass: "PASSWORD"
```

```bash
cd inventory
ansible-playbook -i hosts.yml --ask-vault-pass ../ansible/playbooks/hadoop.yml
```
