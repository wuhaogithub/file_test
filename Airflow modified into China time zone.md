## 1.参考文档

- https://blog.csdn.net/crazy__hope/article/details/83688986

## 2.airflow.cfg

在airflow家目录下修改airflow.cfg，设置

```python
default_timezone = Asia/Shanghai

```

之前在 Centos 安装 AirFlow 的教程中，修改了 airflow.cfg 的时区，以及右上角不显示 UTC 时间格式。但是，运行定时任务实例的时候，发现启动时间完全是不对的，比如我有一个任务放在每天早上 10.30 定时执行，但是运行时间却是在凌晨 2.30 执行的，也就是 UTC 与本地时间的时间差。显然，单单修改 ariflow.cfg 文件是不够的，需要修改源代码进行修正。

## 3.airflow/utils/timezone.py

修改 airflow/utils/timezone.py 文件，在 `utc = pendulum.timezone(‘UTC’)` 这行(第27行)代码下添加:

```python
from airflow import configuration as conf
try:
	tz = conf.get("core", "default_timezone")
	if tz == "system":
		utc = pendulum.local_timezone()
	else:
		utc = pendulum.timezone(tz)
except Exception:
	pass
```

修改utcnow()函数 (在第69行):

```python
原代码 d = dt.datetime.utcnow() 
修改为 d = dt.datetime.now()
```

## 4.airflow/utils/sqlalchemy.py

在 `utc = pendulum.timezone(‘UTC’)` 这行(第37行)代码下添加:

```python
from airflow import configuration as conf
try:
	tz = conf.get("core", "default_timezone")
	if tz == "system":
		utc = pendulum.local_timezone()
	else:
		utc = pendulum.timezone(tz)
except Exception:
	pass
```

修改完成后，再次运行，时间就正确了。