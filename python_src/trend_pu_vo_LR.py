import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import datasets, linear_model
import time
import datetime

# import os, sys
# from sklearn.linear_model import LinearRegression
def get_data(file_name):
    data = pd.read_csv(file_name)
    X_parameter = []
    Y_parameter = []
    for single_hourly, single_volume in zip(data['Hourly'], data['Trading volume']):
        pudt = datetime.datetime.strptime(''.join(single_hourly), '%Y/%m/%d %H:%M')
        volume = int(''.join(single_volume).replace(",", ""))
        X_parameter.append([pudt.time().hour])
        Y_parameter.append(volume)
    return X_parameter,Y_parameter

def linear_model_main(X_parameters, Y_parameters, predict_value):
    regr = linear_model.LinearRegression()
    regr.fit(X_parameters, Y_parameters)
    predict_outcome = regr.predict(predict_value)
    predictions = {}
    predictions['intercept'] = regr.intercept_ #截距b
    predictions['coefficient'] = regr.coef_ #斜率k
    predictions['predicted_value'] = predict_outcome
    return predictions

def show_linear_line(X_parameters,Y_parameters):
    regr = linear_model.LinearRegression()
    regr.fit(X_parameters, Y_parameters)
    plt.scatter(X_parameters,Y_parameters,color='blue')
    plt.plot(X_parameters,regr.predict(X_parameters),color='red',linewidth=4)
    plt.xticks(())
    plt.yticks(())
    plt.show()

if __name__ == '__main__':
    X, Y = get_data('input_data.csv')
    predictvalue = 13
    predictvalue = np.array(predictvalue).reshape(1, -1)
    print(X, Y)
    result = linear_model_main(X, Y, predictvalue)
    print("Intercept = ", result['intercept'])
    print("coefficient = ", result['coefficient'])
    print("Predicted = : ", result['predicted_value'])
    show_linear_line(X, Y)


