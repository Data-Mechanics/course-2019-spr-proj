# import pytz
# from datetime import datetime

# est = pytz.timezone('US/Eastern')
# utc = pytz.utc
# fmt = '%Y-%m-%d %H:%M:%S %Z%z'

# winter = datetime(2016, 1, 24, 18, 0, 0, tzinfo=utc)
# summer = datetime(2016, 7, 24, 18, 0, 0, tzinfo=utc)

# print(winter)
# print(summer)

# print()

# print("2019-04-08T05:01:00-04:00".strftime(fmt))

# print(winter.strftime(fmt))
# print(summer.strftime(fmt))

# print(winter.astimezone(est).strftime(fmt))
# print(summer.astimezone(est).strftime(fmt))

string = "2019-04-08T06:01:00-04:00"

res = string[11:13]
print(res)

timelist = ["06","07","08","09","10","11"]

print(res in timelist)