import matplotlib.pyplot as plt
import dml
import numpy as np


def drawFromDate(date):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
    data_name = 'liweixi_mogujzhu.weather_fire_incident_transformation'

    # initialize fields in weather_fire_incident_transformation
    size = 31
    TAVG = np.zeros(size)
    AWND = np.zeros(size)
    PRCP = np.zeros(size)
    SNOW = np.zeros(size)
    NINCIDENT = np.zeros(size)

    dateStart = date + "-01"
    dateEnd = date + "-32"
    i = 0
    # retrieve data from 2017-01-01 to 2017-01-31
    for col in repo[data_name].find({"DATE":{"$gte":dateStart,"$lt":dateEnd}}):
        TAVG[i] = col['TAVG']
        AWND[i] = col['AWND'] * 1.61
        PRCP[i] = col['PRCP'] * 2.54 * 7
        SNOW[i] = col['SNOW'] * 2.54 * 7
        NINCIDENT[i] = col['NINCIDENT']
        i += 1

    # print(TAVG)
    # print(AWND)
    # print(PRCP)
    # print(SNOW)
    # print(NINCIDENT)

    n = 5   # number of bars in one day
    total_width = 0.8   # set total width of the bars
    plt.rcParams["figure.figsize"] = [14,8]

    # compute the width and position of every bar
    x = np.arange(size) + 1
    width = total_width / n # width for every bar
    x = x - (total_width - width) / 2
    # draw 5 bars per day
    plt.bar(x, TAVG,  width=width, label='TAVG: ℉')
    plt.bar(x + width, AWND,  width=width, label='AWND: km/h')
    plt.bar(x + 2*width, PRCP,  width=width, label='PRCP: mm')
    plt.bar(x + 3*width, SNOW,  width=width, label='SNOW: mm')
    plt.bar(x + 4*width, NINCIDENT, width=width, label='NINCIDENT')
    # set x-axis
    x_axis = np.arange(1,32,1)
    plt.xticks(x_axis)

    plt.title(date + " Weather_Incident")  # set title
    plt.legend()
    # plt.figure(figsize=(16, 10))
    plt.savefig("./weather_incident_histogram_"+date+".png")
    # plt.show()
    plt.cla()

if __name__ == "__main__":
    startDates = []
    years = ['2017', '2018']
    months = ["%.2d" % i for i in range(1,12)]
    for year in years:
        for month in months:
            startDates.append(year+'-'+month)
    # print(startDates)
    # print(endDates)
    for date in startDates:
        drawFromDate(date)