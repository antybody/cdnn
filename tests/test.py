
from pyculiarity import detect_ts
import pandas as pd

import matplotlib.pyplot as plt

twitter_example_data = pd.read_excel('../ele.xlsx',sheet_name='12021-2')

print(twitter_example_data)
plt.figure()

plt.plot(pd.to_datetime(twitter_example_data['TIMESTAMP']),twitter_example_data['FP_TOTALENG'], label=u'first')


#
results = detect_ts(twitter_example_data,
                    max_anoms=0.18,
                    direction='both',e_value=True)

#
print(results)
x = pd.to_datetime(results['anoms']['timestamp'])




plt.plot(x,results['anoms']['anoms'], 'ro',label='check')

# results['anoms']['anoms'].plot()
plt.legend()
plt.show()