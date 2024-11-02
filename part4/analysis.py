import matplotlib
import pandas as pd
from scipy import stats
import seaborn as sns
import matplotlib.pyplot as plt

#* 1. 数据预处理
user_profile = pd.read_csv('user_profile_table.csv')
active_days = pd.read_csv('part-r-00000', delimiter='\t', header=None, names=['user_id', 'active_day'])

# 合并两个数据框
merged_data = pd.merge(user_profile, active_days, on='user_id', how='inner')

# 剔除活跃天数为0的数据
merged_data = merged_data[merged_data['active_day'] > 0]

# 重置索引
merged_data.reset_index(drop=True, inplace=True)

#* 2. 数据的统计分析

print('两表合并后的统计数据展示：')
print('merged_data.head():')
print(merged_data.head())
print('merged_data.describe():')
print(merged_data.describe())


# 数据分组
print('\n将用户根据性别分组，并计算每组的蚂蚁⾦服活跃天数的均值和标准差：')
grouped_data = merged_data.groupby('sex')['active_day'].agg(['mean', 'std', 'count'])
print(grouped_data)

#* 3. 假设检验
print('\n独立样本t检验的假设如下:')
print('零假设（H0）：性别对蚂蚁⾦服活跃天数没有显著影响。')
print('备择假设（H1）：性别对蚂蚁⾦服活跃天数有显著影响。')

# 分别提取男性和女性的活跃天数
male_active_days = merged_data[merged_data['sex'] == 1]['active_day']
female_active_days = merged_data[merged_data['sex'] == 0]['active_day']

# 进行t检验
t_stat, p_value = stats.ttest_ind(male_active_days, female_active_days)

print(f'\n独立样本t检验结果: t-statistic: {t_stat}, p-value: {p_value}')

if p_value < 0.05:
    print('结论：p值小于0.05, 拒绝零假设，说明性别对蚂蚁⾦服活跃天数有显著影响。')
else:
    print('结论：p值大于0.05，不能拒绝零假设，说明性别对蚂蚁⾦服活跃天数没有显著影响。')

#* 4. 可视化展示
# 设置全局字体为中文
matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 黑体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
sns.boxplot(x='sex', y='active_day', data=merged_data)
plt.xticks([0, 1], ['女性', '男性'])
plt.title('按性别分类的活跃天数统计（剔除掉活跃天数为0的数据）')
plt.show()