##########################
## 유저 생성
##########################
useradd str
echo '1' | passwd --stdin str
usermod -G xtractor str
