
import pyspark
import re
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from datetime import datetime

sc = pyspark.SparkContext()

#parse data to LabelPoint object in the format LabelPoint[label,fetures]
def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0],values[1:])

#return data fro dates before 2015
def is_model_date(line):
    try:
        fields = line.split(",")
        date = fields[0].split('-')
        year = int(date[0])
        month = int(date[1])
        volume =fields[2]
        if(volume=='-'):
            return False
        else:
            if(year < 2015):
                #return features for specific months
                # if(month>num1 and month<=num2):
                #     return True
                # else:
                #     return False
                return True
            else:
                return False
    except:
        return False

#return data for dates after 2015
def is_test_date(line):
    try:
        fields = line.split(",")
        date = fields[0].split('-')
        year = int(date[0])
        volume=fields[2]
        month = int(date[1])
        if(volume=='-'):
            return False
        else:
            if(year > 2014):
                #return  features for specific mothns
                # if(month>num1 and month<=num2):
                #     return True
                # else:
                #     return False
                return True
            else:
                return False
    except:
        return False

#change format of volume feature
def volume_size(line):
    fields = line.split(",")
    volume = fields[2]
    total_volume=0
    if 'K' in volume:
            if len(volume) > 1:
                total_volume = float(volume.replace('K', '')) * 1000 # convert k to a thousand
                volume = total_volume
    elif 'M' in volume:
        if len(volume) > 1:
            total_volume = float(volume.replace('M', '')) * 1000000 # convert M to a million
            volume = total_volume

    return("{},{},{},{}".format(fields[0],fields[1],volume,fields[3]))


#bitcoin historical Data from investing.com
data = sc.textFile("/user/ag310/part3/HistoricalData.csv")
#map to ((date),(price,volume))
#convert date format to the same format as transactions.csv date
data = data.map(lambda l: ((datetime.strptime((l.split('"')[1]),'%b %d, %Y').strftime('%Y-%m-%d')),(l.split('"')[3],l.split('"')[11])))

transactions = sc.textFile("/user/ag310/part3/transactions.csv")
#map to (date,transactions)
transactions = transactions.map(lambda l:(l.split(',')[0],l.split(',')[1]))

joined_data = data.join(transactions)
#joined_data => ((date),((price,volume),(transactions)))

#join_data => (date,price,volume,transactions)
join_data = joined_data.map(lambda l: "{},{},{},{}".format(l[0],l[1][0][0].replace(",",""),l[1][0][1],l[1][1]))
#join_data dates is between 2010-07-18 - 2018-11-30

#change format of volume variable and convert to float
join_data = join_data.map(volume_size)



#Data to use for training the model
#get the data from 2015 and before
model_data = join_data.filter(is_model_date)
#change to new format(label,features) and remove the date variable
model_data = model_data.map(lambda l:"{},{},{}".format(l.split(",")[1],l.split(",")[2],l.split(",")[3]))


#Data to use for testing the model
#get the data from 2015 to 2018
testing_data = join_data.filter(is_test_date)
#change to new format(label,features) and remove the date variable
testing_data = testing_data.map(lambda l:"{},{},{}".format(l.split(",")[1],l.split(",")[2],l.split(",")[3]))

#use parsePoint to return data in the format of (lalel,features...)
training_parsedData = model_data.map(parsePoint)
testing_parsedData = testing_data.map(parsePoint)

##exrtra method used for testing dataset from the before2015 dataset
#training,testing = training_parsedData.randomSplit([0.8,0.2])


##train model based on training dataset
model = LinearRegressionWithSGD.train(training_parsedData,iterations=100,step=0.0000000001)
##evaluate on training data
valuesAndPreds = testing_parsedData.map(lambda p: (p.label, model.predict(p.features)))
#caclulate RMSE
MSE = valuesAndPreds.map(lambda v: (v[0] - v[1])**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
#take sample to show actual prices and predictions of model
sample = valuesAndPreds.takeSample(False,30)
for line in sample:
    print(line)
print("Root Mean Squared Error = " + str(MSE))
