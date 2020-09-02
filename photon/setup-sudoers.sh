#!/bin/bash

echo presto | xargs -n 1 useradd -r -m -s /bin/bash -g udocker  && sed -i -e 's/%sudo\s\+ALL=(ALL:ALL)\s\+ALL/%sudo ALL=(ALL) NOPASSWD:ALL/g' /etc/sudoers
echo presto | xargs -n 1 usermod -a -G sudo,udocker && sed -i -e 's/%sudo\s\+ALL=(ALL:ALL)\s\+ALL/%sudo ALL=(ALL) NOPASSWD:ALL/g' /etc/sudoers
echo udocker | xargs -n 1 usermod -a -G sudo,udocker && sed -i -e 's/%sudo\s\+ALL=(ALL:ALL)\s\+ALL/%sudo ALL=(ALL) NOPASSWD:ALL/g' /etc/sudoers
usermod -a -G udocker presto

exit 0
