
import pandas as pd
import numpy as np
file = "xnym-bbo-cl-fut-20190114-r-00103"
from sorter import csvsort
import numpy


def convert_date_time(timestamp):
    DTS = []
    YY = timestamp[:4]
    DTS.append(YY)
    MM = timestamp[4:6]
    DTS.append(MM)
    DD = timestamp[6:8]
    DTS.append(DD)
    HH = timestamp[8:10]
    DTS.append(HH)
    mm = timestamp[10:12]
    DTS.append(mm)
    ss = timestamp[12:14]
    DTS.append(ss)
    ms = timestamp[14:]
    DTS.append(ms)
    return "-".join(str(i) for i in DTS[:3]) + " " + ":".join(str(j) for j in DTS[3:-1]) + "." + str(ms)

def read_tick(file):
    f = open(file,"r")
    fw = open("paresd_tick.csv","w")
    fw.write(",".join(["Datetime","Price","Volume"])+"\n")
    lines = f.readlines()
    df = parse_contents(lines)
    df = df.sort_values(by="Datetimestamp")
    df.reset_index(drop=True,inplace=True)

    print(df.index)
    df.to_csv("intermediate_output_2.csv")
    group_data(df)

def group_data(df):
    df_n = pd.pivot_table(df[30:], values='Future_Price', index=['Datetimestamp'], aggfunc=np.unique)
    dff = df_n.reset_index(drop=False)
    dates = dff.Datetimestamp.tolist()
    prices = dff.Future_Price.tolist()
    date_ = []
    price_ = []
    try:

        for i in range(len(dates)):
            millis = 0
            if isinstance(prices[i], str):
                millis+=300
                date_.append(convert_date_time(str(dates[i]+str(millis))))
                price_.append(prices[i])
            else:
                for j in range(len(prices[i])):
                    millis += 300
                    date_.append(convert_date_time(str(dates[i]+str(millis))))
                    price_.append(prices[i][j])
        data_frame = pd.DataFrame({'Datetimestamp': date_,
                                   'Future_Price': price_,
                                   })
        data_frame.to_csv("final_output_2.csv")
        csvsort("final_output_1.csv", [1], has_header=True)
    except Exception as e:
        print(str(e))
def parse_contents(lines):
    datetime=[]
    prices=[]
    volume=[]
    sym_table = {"20181114": "1901", "20181214": "1902", "20190116": "1903"}
    for line in lines:
        data = str(line)
        date = data[0:8]
        time = data[8:14]
        trade_price = data[44:51]
        traded_volume = data[31:36]
        #print(int(traded_volume))
        decimal = data[51:52]
        ask_bid = data[52:53]
        expiry = data[27:31]
        if int(date) <= int(list(sym_table.keys())[0]):
            sym = "1812"
        elif int(date) > int(list(sym_table.keys())[0]) and int(date) <= int(list(sym_table.keys())[1]):
            sym = sym_table[list(sym_table.keys())[0]]
        elif int(date) > int(list(sym_table.keys())[1]) and int(date) <= int(list(sym_table.keys())[2]):
            sym = sym_table[list(sym_table.keys())[1]]
        else:
            sym = sym_table[list(sym_table.keys())[2]]

        price = trade_price[3:-int(decimal)]+'.'+trade_price[-int(decimal):]
        if ask_bid not in ["B","A"] and expiry == sym :
            datetime.append(str(date+time))
            prices.append(price)
            volume.append(int(traded_volume))
    data_frame = pd.DataFrame({'Datetimestamp':datetime,
                                    'Future_Price':prices,
                                     'Volume':volume})

    return data_frame


def filter_ticks():
    df = pd.read_csv('paresd_tick.csv')
    df_new = df.loc[(df["Datetime"] > 20190131090000) & (df["Datetime"] < 20190131113000)]
    print(df_new)
    df_new.to_csv("filtered.csv")


if __name__=="__main__":
    read_tick(file)