# cdnn
## models/median 四分位算法
  
## models/clof 聚类

## models/arima 指数平滑

## models/smooth 滑动平均

## pyculiarity/时间序列 

## predata/pre_lof 数据遗漏处理

## tests/test 测试脚本

## db 访问数据库的配置

## algorithm 最终方法


## 环境配置
 -- python 3.7  <br>
 -- 相关工具安装包 见 执行config 下 pyInstall <br>
 -- 重点 ox_Oracle 需要配置环境变量（这个可自行百度，windows 和 mac 配置方法不同）<br>
 
## python 在java 项目中的配置
 -- 1、python 包 随意放在java 项目内  <br>
 -- 2、在跟 python 包 同根目录下放置 run.py 文件  <br>
 -- 3、java 代码里 访问 run.py 即可（run.py 文件 在java 目录下）

## JAVA 接口访问
 -- 见java 文件夹内的示例代码 <br>
 -- 具体见代码 <br>
 -- 传递的参数 主要是 时间范围（start,end） + 检测算法 （diff -- 默认就是差分）

## 数据库说明
 -- error_in  <br>
 -- error_out  dt_edit 是 修正后的值，dt_val 原始值 ,dt_reason 修改原因 0 正常，1 缺失值 2 负值 3 零 4 极值






