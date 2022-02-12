import datetime as dt

epoch = 1632201592
print(epoch, '-------------mi- epoch')

mytime = dt.datetime.fromtimestamp(epoch)
print(mytime, '----- converted')

myutc = mytime.strftime('%Y-%m-%dT%H:%M:%SZ')
print(myutc, '---- utc time')

mygmt7 = mytime.strftime("%Y-%m-%dT%H:%M:%S+07:00")
print(mygmt7, 'gmt+7 time')