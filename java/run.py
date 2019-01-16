import sys
from cdnn.algorithm.algorithm import *


#方法选择
def fun_choice(func,starttime,endtime):
    parse_func = {
        'diff': data_list(starttime,endtime)
    }
    parse_func[func]  # 执行相应方法

if __name__ ==  '__main__':
    fun_choice(sys.argv[1],sys.argv[2],sys.argv[3])











