# created by hyz in 2023.04.06

# 任务：将gaia数据导入mysql数据库

可选方式：
逐条insert，优点：可以任意用户导入，不受安全保护，容易并行。缺点：慢，不安全。
整个文件import，优点：快。缺点：受安全保护，安全限制难以修改，需要root用户权限及反复重启，若将数据导入安全文件夹将只有root用户能够操作文件夹，因此难以并行。

分别对应文件和sh：
mpicsv2db.py sql.sh output.py
test_mpicsv2db2.py sql2.sh output2.py

缓存问题：
当写入速度超过读取速度时，很容易因数据分离导致碎片化造成脏页，从而使缓存量堆积，直至满内存导致系统变慢。因此mysql导入数据过程中需要定期清理缓存。清理缓存的sh文件为~/clean_cache.sh

目前的数据文件位于：
/var/lib/mysql-files
