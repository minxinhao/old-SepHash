import re
import csv
import argparse

# 创建一个解析器对象
parser = argparse.ArgumentParser()

# 添加需要解析的参数
parser.add_argument('num_clients', type=int, help='number of clients')

# 解析命令行参数
args = parser.parse_args()

# 获取解析后的参数值
num_clients = args.num_clients

# 根据参数值选择对应的输入文件内容
if num_clients == 2:
    input_files = ['out192.168.1.51.txt', 'out192.168.1.52.txt']
elif num_clients == 4:
    input_files = ['out192.168.1.51.txt', 'out192.168.1.52.txt', 'out192.168.1.53.txt', 'out192.168.1.33.txt']
elif num_clients == 8:
    input_files = ['out192.168.1.51.txt', 'out192.168.1.52.txt', 'out192.168.1.53.txt', 'out192.168.1.33.txt', 'out192.168.1.44.txt', 'out192.168.1.69.txt', 'out192.168.1.88.txt', 'out192.168.1.89.txt']
print(input_files)

# 定义正则表达式来匹配Load IOPS和Run IOPS的数据
load_iops_regex = r'Load IOPS:(\d+\.\d+)Kops'
run_iops_regex = r'Run IOPS:(\d+\.\d+)Kops'

# 创建一个字典用于存储所有输入文件的数据提取结果
results = {}

# # 指定所有输入文件的名称或路径
# input_files = ['out192.168.1.51.txt', 'out192.168.1.52.txt']
# # input_files = ['out192.168.1.51.txt', 'out192.168.1.52.txt','out192.168.1.53.txt', 'out192.168.1.33.txt']
# # input_files = ['out192.168.1.51.txt', 'out192.168.1.52.txt','out192.168.1.53.txt', 'out192.168.1.33.txt','out192.168.1.44.txt', 'out192.168.1.69.txt','out192.168.1.88.txt', 'out192.168.1.89.txt']

# 遍历所有的输入文件
for input_file in input_files:
    # 打开输入文件并读取所有内容
    with open(input_file, 'r') as f:
        content = f.read()

    # 查找所有匹配的Load IOPS和Run IOPS数据
    load_iops_matches = re.findall(load_iops_regex, content)
    run_iops_matches = re.findall(run_iops_regex, content)

    # 将提取的数据添加到结果字典中
    for i in range(len(load_iops_matches)):
        key = 'Load IOPS ' + str(i+1)
        if key not in results:
            results[key] = float(load_iops_matches[i])
        else:
            results[key] += float(load_iops_matches[i])

        key = 'Run IOPS ' + str(i+1)
        if key not in results:
            results[key] = float(run_iops_matches[i])
        else:
            results[key] += float(run_iops_matches[i])

# 将所有输入文件的结果写入CSV文件
with open('output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Metric', 'Value'])
    for key in results:
        if key.startswith('Load IOPS'):
            writer.writerow([key, results[key]])
    for key in results:
        if key.startswith('Run IOPS'):
            writer.writerow([key, results[key]])