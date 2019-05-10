import os
import matplotlib.pyplot as plt

def plot(question):
    strFile = './static/images/img.jpg'
    if os.path.isfile(strFile):
        os.remove(strFile)
    reps = question[1::2]
    dems = question[0::2]
    plt.figure()
    plt.scatter([x[0] for x in reps], [x[1] for x in reps], c='red')
    plt.scatter([x[0] for x in dems], [x[1] for x in dems], c='blue')
    plt.xlabel('For')
    plt.ylabel('Against')
    plt.title('Republicans vs. Democrats on this topic')
    plt.xlim(0, 100)
    plt.ylim(0, 100)
    plt.savefig('static/images/img.jpg', bbox_inches='tight')
    plt.close()

def plot2(coords, x, y):
    x = list(x)
    y = list(y)
    x = x[1:]
    y = y[1:]
    strFile = './static/images/img2.jpg'
    if os.path.isfile(strFile):
        os.remove(strFile)
    plt.figure()
    #ax = f.subplots()
    plt.scatter(x, y, c='k')
    plt.scatter(coords[0], coords[1], color='m')
    plt.xlabel('Carbon Efficacy')
    plt.ylabel('Emissions Per Capita')
    plt.title('Emissions per Capita over Carbon Efficacy')
    plt.savefig('static/images/img2.jpg')
    plt.close()