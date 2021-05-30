import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import datetime

def get_data(file_name):
    data = pd.read_csv(file_name)
    X_parameter = []
    Y_parameter = []
    for single_hourly, single_volume in zip(data['Hourly'], data['Trading volume']):
        pudt = datetime.datetime.strptime(''.join(single_hourly), '%Y/%m/%d %H:%M')
        volume = int(''.join(single_volume).replace(",", ""))
        X_parameter.append(pudt.time().hour)
        Y_parameter.append(volume)
    return X_parameter,Y_parameter

def calculate_work(x_data, y_data):  # 回归方程中b0、b1的求解
    x_mean = np.mean(x_data)
    y_mean = np.mean(y_data)
    x1 = x_data - x_mean
    y1 = y_data - y_mean
    s = x1 * y1
    u = x1 * x1
    b1 = np.sum(s) / np.sum(u)
    b0 = y_mean - b1 * x_mean
    return b1, b0


def test_data_work(b1, b0, text_data):  # 回归方程的建立与数值预测
    result = list([])
    for one_test in text_data:
        y = b0 + b1 * one_test
        result.append(y)
    return result


def root_data_view(x_data, y_data):  # 绘制源数据可视化图
    plt.scatter(x_data, y_data, label='simple regress', color='k', s=5)  # s 点的大小
    plt.xlabel('x')
    plt.ylabel('y')
    plt.legend()
    plt.show()
    return


def test_data_view(x_data, y_data):  # 绘制回归线
    # 绘制回归线两个点的数据
    x_min = np.min(x_data)
    x_max = np.max(x_data)
    y_min = np.min(y_data)
    y_max = np.max(y_data)
    x_plot = list([x_min, x_max])
    y_plot = list([y_min, y_max])
    # 绘制
    plt.scatter(x_data, y_data, label='root data', color='k', s=5)  # s 点的大小
    plt.plot(x_plot, y_plot, label='regression line')
    plt.xlabel('x')
    plt.ylabel('y')
    plt.legend()
    plt.title('simple linear regression')
    plt.show()
    return

if __name__ == '__main__':
    X, Y = get_data('input_data.csv')
    test_data = list([13])
    b1, b0 = calculate_work(X, Y)
    result = test_data_work(b1, b0, test_data)
    print("b0 = ", b0)
    print("b1 = ", b1)
    print("Predicted = ",result)
    test_data_view(X, Y)