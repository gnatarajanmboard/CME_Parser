import blaze
import pandas as pd
import numpy as np
from BTrees.OOBTree import OOBTree
import gc
import sys
from csvsort import csvsort
import copy
from scipy.stats import skew
from scipy.stats import kurtosis

class MDUpdateAction(object):
    NEW = '0'
    CHANGE = '1'
    DELETE = '2'
    OVERLAY = '5'

class MDEntryType(object):
    BID = '0'
    OFFER = '1'
    TRADE = '2'


class Message(object):
    APPLVERID               = '1128'
    BODYLENGTH              = '9'
    LASTMSGSEQNUMPROCESSED  = '369'
    MDSECURITYTRADINGSTATUS = '1682'
    MSGSEQNUM               = '34'
    MSGTYPE                 = '35'
    POSSDUPFLAG             = '43'
    SENDERCOMPID            = '49'
    SENDINGTIME             = '52'
    AGGRESSORSIDE       = '5797'
    FIXINGBRACKET       = '5790'
    MATCHEVENTINDICATOR = '5799'
    MDENTRYPX           = '270'
    MDENTRYSIZE         = '271'
    MDENTRYTIME         = '273'
    MDENTRYTYPE         = '269'
    MDPRICELEVEL        = '1023'
    MDQUOTETYPE         = '1070'
    MDUPDATEACTION      = '279'
    NETCHGPREVDAY       = '451'
    NOMDENTRIES         = '268'
    NUMBEROFORDERS      = '346'
    OPENCLOSESETTLEFLAG = '286'
    QUOTECONDITION      = '276'
    RPTSEQ              = '83'
    SECURITYDESC        = '107'
    SECURITYID          = '48'
    SECURITYIDSOURCE    = '22'
    SETTLDATE           = '64'
    SYMBOL              = '55'
    TICKDIRECTION       = '274'
    TRADECONDITION      = '277'
    TRADEDATE           = '75'
    TRADEVOLUME         = '1020'
    TRADINGSESSIONID    = '336'
    DELIMITER           = '\x01'
    DELIMITER = ','
    def __init__ (self,msg):
        #self.pp = pprint.PrettyPrinter(depth=10)
        self.header = {}
        self.repeatinggroups = []
        self.message = msg



    def isIncremental(self):
        s = self.message.split(Message.DELIMITER)
        s.pop(-1)
        #p = dict( z.split('=') for z in s )
        p = dict(z.split('=') for z in s if "=" in z)
        return p.get('35') == 'X'

    def parse(self):
        header,repeatgroups = self.message.split('268=')
        self.__parseHeader(header)
        self.__parseRepeatingGroups(repeatgroups)
        #print header

    def __parseHeader (self,header):
        w=header.split(Message.DELIMITER)
        w.pop(-1)
        for elem in w:
            if "=" in elem:
                k,v=  elem.split('=')
            else:
                k = '52'
                v = elem
            self.header[k] = v
        #self.pp.pprint(self.header)

    def __parseRepeatingGroups (self,rg):
        groups = rg.split('279=')
        groups.pop(0)
        #print len(groups)
        for g in groups:
            g= '279=' + g
            w=g.split(Message.DELIMITER)
            w.pop(-1)
            mydict={}
            for elem in w:
                k,v=  elem.split('=')
                mydict[k] = v
            self.repeatinggroups.append(mydict)
        #self.pp.pprint(self.repeatinggroups)

    def getSENDINGTIME(self):
        return self.header[Message.SENDINGTIME]

    def getMDENTRIES (self):
        return self.repeatinggroups

    def getMDPRICE (self):
        return self.header[Message.SENDINGTIME]

    def getNOMDENTRIES (self):
        return len(self.repeatinggroups)

    def getSorted (self):
        return sorted(self.repeatinggroups,key=lambda x: x[Message.MDENTRYPX])

    def getNew (self):
        """
        return indexes of new orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.NEW:#and Message.QUOTECONDITION not in ha.keys():
                idx.append(i)
        return idx

    def getChange (self):
        """
        return indexes of new orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.CHANGE: #and Message.QUOTECONDITION not in ha.keys():
                idx.append(i)
        return idx

    def getDelete (self):
        """
        return indexes of deleted orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.DELETE and Message.QUOTECONDITION not in ha.keys():
                idx.append(i)
        return idx




class Level(object):
    def __init__(self,kwargs,delete = False):
        if not delete:
            self.level = int(kwargs[Message.MDPRICELEVEL])
            self.price = kwargs[Message.MDENTRYPX]
            self.size  = kwargs[Message.MDENTRYSIZE]
            self.side  = kwargs[Message.MDENTRYTYPE]
            #self.seqId = kwargs[Message.MSGSEQNUM]
        else:
            self.level = int(kwargs[Message.MDPRICELEVEL])
            self.price = 0
            self.size = 0
            self.side = kwargs[Message.MDENTRYTYPE]






class FileUtils(object):
    def __init__ (self,filename, depth):
        self.filename = filename
        self.depth = depth
        self.chunklen = 1
        self.df = self.createDataFrame()
        #write header
        self.df.to_csv(self.filename,index=False)
        self.rowcounter = 0


    def createDataFrame (self):
        self.cols =['ticker'
                ,'timestamp'
                ,'bid1'
                ,'ask1'
                ,'bidsize1'
                ,'asksize1'
                ,'bid2'
		         ,'ask2'
                ,'bidsize2'
                ,'asksize2'
                ,'bid3'
                ,'ask3'
                ,'bidsize3'
                ,'asksize3'
            , 'bid4'
	    , 'ask4'
            , 'bidsize4'
            , 'asksize4'
            , 'bid5'
 	    , 'ask5'
            , 'bidsize5'
            , 'asksize5'
            , 'bid6'
	    , 'ask6'
            , 'bidsize6'
            , 'asksize6'
            , 'bid7'
	    , 'ask7'
            , 'bidsize7'
            , 'asksize7'
            , 'bid8'
	    , 'ask8'
            , 'bidsize8'
            , 'asksize8'
            , 'bid9'
	    , 'ask9'
            , 'bidsize9'
            , 'asksize9'
            , 'bid10'
	    , 'ask10'
            , 'bidsize10'
            , 'asksize10'
            , 'Volume_Bid'
            ,'Volume_Ask'
            ,'Skew_Bid'
            ,'Skew_Ask'
            ,'Kurtosis_Bid'
            ,'Kurtosis_Ask'
                    ]
        return blaze.DataFrame(columns=self.cols)

    def convert_date_time(self,timestamp):
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

    def add_new(self,bidbook,askbook,ticker,timestamp):
        self.rowcounter+=1
        bids = np.zeros(self.depth)
        asks = np.zeros(self.depth)
        bidsize = np.zeros(self.depth)
        asksize = np.zeros(self.depth)
        buys = [[bidbook[b].price, bidbook[b].size] for b in sorted(bidbook.keys())]
        sells = [[askbook[a].price, askbook[a].size] for a in sorted(askbook.keys())]
        buys = np.array(buys)
        sells = np.array(sells)
        bids[0:len(buys)] = buys[:, 0][:self.depth]
        asks[0:len(sells)] = sells[:, 0][:self.depth]
        bidsize[0:len(buys)] = buys[:, 1][:self.depth]
        asksize[0:len(sells)] = sells[:, 1][:self.depth]
        sorted_ = self.is_sorted(bids.tolist(),asks.tolist())
        unique = self.is_unique(bids.tolist(),asks.tolist())
        if unique:
            if sorted_:
                self.get_and_write_data(ticker,bids,asks,bidsize,asksize,timestamp)
            else:
                pass
        else:
            pass

    def is_unique(self,bids,asks):
        return True if len(bids) == len(set(bids))  and len(asks) == len(set(asks)) else False

    def is_sorted(self,bids,asks):
        bid_copy = copy.copy(bids)
        ask_copy = copy.copy(asks)
        bid_copy[:] = (value for value in bid_copy if value != 0)
        ask_copy[:] = (value for value in ask_copy if value != 0)
        bid_sorted = True if bid_copy == sorted(bid_copy,reverse=True) else False
        ask_sorted = True if ask_copy == sorted(ask_copy) else False
        return bid_sorted and ask_sorted


    def tocsv(self):
        self.df.to_csv(self.filename,index=False,mode = 'a',header=False)

    def add (self,bidbook,askbook,ticker,timestamp):
        self.rowcounter+=1
        bids = [0]*self.depth
        asks = [0]*self.depth
        bidsize = [0]*self.depth
        asksize = [0]*self.depth
        buys  = [ [bidbook[b].price, bidbook[b].size, bidbook[b].level] for b in sorted(bidbook.keys()) ]
        sells = [ [askbook[a].price, askbook[a].size, askbook[a].level] for a in sorted(askbook.keys()) ]
        buys  = np.array(buys)
        sells = np.array(sells)
        if len(buys) > 0:
            prices = list(buys[:,0][:self.depth])
            sizes = list(buys[:, 1][:self.depth])
            depths = list(buys[:, 2][:self.depth])
            for dep in range(len(depths)):
                bids[int(depths[dep])-1] = prices[dep]
                bidsize[int(depths[dep])-1] = sizes[dep]
        else:
            bids = np.zeros(self.depth)
            bidsize = np.zeros(self.depth)
        if len(sells) > 0:
            prices = list(sells[:, 0][:self.depth])
            sizes = list(sells[:, 1][:self.depth])
            depths = list(sells[:, 2][:self.depth])
            for dep in range(len(depths)):
                asks[int(depths[dep])-1] = prices[dep]
                asksize[int(depths[dep])-1] = sizes[dep]
        else:
            asks = np.zeros(self.depth)
            asksize = np.zeros(self.depth)


    def get_and_write_data(self,ticker,bids,asks,bidsize,asksize,timestamp):
        timestamp = self.convert_date_time(timestamp)

        data = [ticker
            , timestamp
            , bids[0]
            , asks[0]
            , bidsize[0]
            , asksize[0]
            , bids[1]
            , asks[1]
            , bidsize[1]
            , asksize[1]
            , bids[2]
            , asks[2]
            , bidsize[2]
            , asksize[2]
            , bids[3]
            , asks[3]
            , bidsize[3]
            , asksize[3]
            , bids[4]
            , asks[4]
            , bidsize[4]
            , asksize[4]
            , bids[5]
            , asks[5]
            , bidsize[5]
            , asksize[5]
            , bids[6]
            , asks[6]
            , bidsize[6]
            , asksize[6]
            , bids[7]
            , asks[7]
            , bidsize[7]
            , asksize[7]
            , bids[8]
            , asks[8]
            , bidsize[8]
            , asksize[8]
            , bids[9]
            , asks[9]
            , bidsize[9]
            , asksize[9]
            , np.sum(bidsize)
            ,np.sum(asksize)
            ,skew(bidsize)
            ,skew(asksize)
            ,kurtosis(bidsize)
            ,kurtosis(asksize)
                ]
        ds = pd.Series(data, index=self.cols)
        self.df = self.df.append(ds, ignore_index=True)
        if self.rowcounter % self.chunklen == 0:
            self.tocsv()
            gc.collect()
            self.df = self.createDataFrame()

    def tocsv(self):
        self.df.to_csv(self.filename,index=False,mode = 'a',header=False)


class Parser(object):
    def __init__ (self,file,**kwargs):
        self.file = file
        self.fh = open(self.file, "r")
        self.symbol = kwargs['symbol']
        self.depth = kwargs['depth']
        self.bidbook = OOBTree()
        self.askbook = OOBTree()
        self.fileutils = FileUtils('cme_out_agg_Feb22.csv',10)
        self.data = 'data_4_v2.csv'
        self.data_file = open(self.data,"w")
        self.updateBook = False
        self.prices = {}
        self.prices['Bid'] = [0]*10
        self.prices['Ask'] = [0]*10

    def filter_data(self):
        for line in self.fh:
            if "CLQ3" in line:
                split = line.split("\x01")
                #print(split)
                c = [x.replace('52=', '') if '52=' in x else x for x in split]
                n_l = (",".join(x for x in c))
                self.data_file.write(n_l)
        self.data_file.close()
        csvsort(self.data, [5], max_size=250, has_header=False)
        print("Sorting and filtering done ")


    def read(self):
        print("Parsing data")
        f_p = open(self.data, "r")
        for line in f_p:
            msg = Message(line)
            if not msg.isIncremental(): continue
            msg.parse()
            mdentries= msg.getMDENTRIES()
            for i in msg.getDelete():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    try:
                       self.bidbook[ int(mdentries[i][Message.MDPRICELEVEL])-1 ] = Level(mdentries[i],True)
                    except:
                        pass
                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    try:
                        self.askbook[ int(mdentries[i][Message.MDPRICELEVEL])-1 ] = Level(mdentries[i],True)
                    except:
                        pass
            for i in msg.getChange():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    self.bidbook[int(mdentries[i][Message.MDPRICELEVEL])-1] = Level(mdentries[i])
                    print([[self.bidbook[b].price, self.bidbook[b].size, self.bidbook[b].level] for b in
                           sorted(self.bidbook.keys())])
                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    self.askbook[ int(mdentries[i][Message.MDPRICELEVEL])-1]= Level(mdentries[i])

            for i in msg.getNew():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    try:
                        self.bidbook[int(mdentries[i][Message.MDPRICELEVEL])-1] = Level(mdentries[i])

                    except:
                        print("Error Here -- IF BID -- NEW")
                        print("Unexpected error:", sys.exc_info()[0])
                        pass

                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    try:
                        self.askbook[int(mdentries[i][Message.MDPRICELEVEL])-1 ]= Level(mdentries[i])

                    except:
                        print("Error Here -- ELSE ASK -- NEW")

                        print ("Unexpected error:", sys.exc_info()[0])

            #self.display()
            try:

                self.fileutils.add_new(self.bidbook,self.askbook,self.symbol,msg.getSENDINGTIME())
            except Exception as e:
                print(str(e))


    def display(self):
        try:
            del self.askbook[ '10' ]
        except:
            pass
        try:
            del self.bidbook[ '10' ]
        except:
            pass
        print ('================================================================\\')
        print(self.askbook.keys())
        for a in sorted(self.askbook.keys(), reverse=True):
            print (self.askbook[a].price , self.askbook[a].size)
        print ("@@")
        print(self.bidbook.keys())
        for b in sorted(self.bidbook.keys()):
            print(self.bidbook[b].price , self.bidbook[b].size)
        print ('================================================================/')
        print



    def __del__ (self):
        self.fh.close()



def sort_csv(file):
    csvsort(file, 1)


if __name__ == '__main__':
    f='mdff_nym_20130714-20130715_7815_0'
    sym='CLQ3'
    p = Parser(f,symbol=sym,depth='10')
    p.filter_data()
    p.read()