# houzz-common-lib
common lib for houzz developers

## setup ~/.pypirc
```
[distutils]
index-servers =
  pypi
  local

[pypi]
username:platform
password:platform
repository:http://192.168.10.237:8081
```

## setup ~/.pip/pip.conf
```
[global]
extra-index-url = http://platform:platform@192.168.10.237:8081
trusted-host = 192.168.10.237
```


Since the password are stored in plain text, please at least set the permission, so that only you can view or modify it

chmod 600 ~/.pypirc ~/.pip/pip.conf