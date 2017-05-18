#Custom Yahoo Data Analysis Tool Package
#Edit Notes: Verify risk adjusted returns code and such is correct.
    # Find a better way to resample in getYahoo
    # Change riskAdjReturns to return a percentage number. Need to add maxRisk input and get %win/loss values. 

import pandas as pd
import numpy as np

#Print Content Guide
def getContents():
    print('getYahoo, openDev, correlation, riskAdjustedReturns, highLowRange, closeToOpen)')

#Granularity Options: 'd' = daily, 'w' = weekly, 'm' = yearly
#Verified: Basic retrieval and dateTime are verified. ReSample has not been verified
def getYahoo(ticker, granularity, startMonth, startDay, startYear,
             endMonth, endDay, endYear, parseDates = False,
             resample = False):
    
    startMonth -= 1
    endMonth -= 1
    url = ("http://chart.finance.yahoo.com/table.csv?s={}&a={}&b={}&c={}&d={}&e={}&f={}&g={}&ignore=.csv".
           format(ticker, startMonth, startDay, startYear, endMonth, endDay, endYear, granularity))

    #dateTime Conversion
    if parseDates == False:
        df = pd.read_csv(url)
    else:
        df = pd.read_csv(url, parse_dates = [0], index_col = [0])

    #Resample
    if resample == False:
        pass
    else:
        r = df
        #Sample for every n minutes
        n = resample

        #RESAMPLE BY COLUMN

        #Set Beginning var 'b' necessary for some column references.
        b = n-1

        #Date
        r_date = pd.DataFrame(r['Date'])
        date = r_date.iloc[b::n, :]
        date = date.reset_index(drop=True)

        #Open
        r_open = pd.DataFrame(r['Open'])
        open = r_open.iloc[::n, :]
        open = open.reset_index(drop=True)


        #High
        high = pd.Series([chunk.max() for chunk in np.array_split(r['High'], range(n, len(r['High']), n))])
        high = high.reset_index(drop=True)

        #Low
        low = pd.Series([chunk.min() for chunk in np.array_split(r['Low'], range(n, len(r['Low']), n))])
        low = low.reset_index(drop=True)

        #Close
        r_close = pd.DataFrame(r['Close'])
        close = r_close.iloc[b::n, :]
        close = close.reset_index(drop=True)

        #Volume
        volume = pd.Series([chunk.sum() for chunk in np.array_split(r['Volume'], range(n, len(r['Volume']), n))])
        volume = volume.reset_index(drop=True)

        #Concat columns to 'resampled'
        joined = [date, open, high, low, close, volume]

        resampled = pd.concat(joined, axis=1)
        resampled.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

        df = resampled          
            
    return df


def openDev(df, filterSet = False):
    #Calculate the simple probability of a given deviation from the open price.
    #Goal is to return a df with probabilities of given move above or below open in percentage terms.

    if filterSet == False:
        filterSet = [-.05, -.045, -.04, -.035, -.03, -.025, -.02, -.0175, -.015, -.0125, -.01
                -.009, -.008, -.007, -.006, -.005, -.004, -.003, -.002, -.001, 0, .001,
                .002, .003, .004, .005, .006, .007, .008, .009, .01, .0125, .015, .0175, .02,
                .025, .03, .035, .04, .045, .05]
    else:
        pass

    df['highDev'] = (df.High - df.Open)/df.Open
    df['lowDev'] = (df.Low - df.Open)/ df.Open
    df['closeDev'] = (df.Close - df.Open)/df.Open

    dfOpenDev = pd.DataFrame(index = filterSet)
    highDevCol = []
    lowDevCol = []
    closeDevGreater = []
    closeDevLess = []
    
    for x in filterSet:
        highDevCol.append(len(df[df.highDev > x]) / len(df))
        lowDevCol.append(len(df[df.lowDev < x]) / len(df))
        closeDevGreater.append(len(df[df.closeDev > x])/ len(df))
        closeDevLess.append(len(df[df.closeDev < x]) / len(df))
            

    dfOpenDev['highDev'] = highDevCol
    dfOpenDev['lowDev'] = lowDevCol
    dfOpenDev['closeDevGreater'] = closeDevGreater
    dfOpenDev['closeDevLess'] = closeDevLess

    return dfOpenDev

def correlation(ticker1, ticker2, granularity, startMonth, startDay, startYear,
             endMonth, endDay, endYear, index=False):
    #Continued Improvement Notes: Improve data output format.
        #Add note detailing what this actually does. 
    
    stringTicker1 = str(ticker1)
    stringTicker2 = str(ticker2)

    df1 = getYahoo(ticker1, granularity, startMonth, startDay,
                   startYear, endMonth, endDay, endYear, index)
    df2 = getYahoo(ticker2, granularity, startMonth, startDay,
                   startYear, endMonth, endDay, endYear, index)

    tickerOnePercentChange = (df1['Close'] - df1['Open']) / df1['Close'] * 100
    tickerTwoPercentChange = (df2['Close'] - df2['Open']) / df2['Close'] * 100

    df3 = pd.DataFrame()
    df3[stringTicker1] = tickerOnePercentChange
    df3[stringTicker2] = tickerTwoPercentChange
    df3.to_pickle("pickle.pickle")
    df = pd.read_pickle("pickle.pickle")
    dfCorr = df.corr()

    directionalCorr = 0
    n = 0
    while n < len(df3):
        if df3[stringTicker1][n] > 0 and df3[stringTicker2][n] > 0:
            directionalCorr += 1
            n+=1
        elif df3[stringTicker1][n] < 0 and df3[stringTicker2][n] < 0:
            directionalCorr +=1
            n+=1
        else:
            n+=1
    directionalCorrPercent = directionalCorr / len(df3) * 100

    df4 = pd.DataFrame()
    df4[stringTicker1+' ABS'] = abs(df3[stringTicker1])
    df4[stringTicker2+' ABS'] = abs(df3[stringTicker2])

    absDfCorr = df4.corr()
        
    return [dfCorr, absDfCorr, 'Directional Correlation %:', directionalCorrPercent, '%']

def riskAdjustedReturns(df, currentPrice, target1, criteria, win,
                        loss, target2 = False, chanceOnly = False):
    #Assumed standard Yahoo DF
    #Criteria Options:
        #Single Target: hit, notHit, closeOver, closeUnder
        #Double Target: closeBetween, hitOne, hitBoth

    percentMove = (target1 - currentPrice)/ currentPrice

    if target2 == False:
        pass
    else: 
        percentMove2 = (target2 - currentPrice)/ currentPrice
        if percentMove > 0 and percentMove2 > 0:
            print('Target1 and Target 2 cannot both be > current price.')
        elif percentMove < 0 and percentMove2 < 0:
            print('Target1 and Target 2 cannot both be < current price.')
        else:
            pass

    df['highDev'] = (df.High - df.Open)/df.Open
    df['lowDev'] = (df.Low - df.Open)/ df.Open
    df['closeDev'] = (df.Close - df.Open)/df.Open


    if criteria == 'closeOver':
        criteriaMet =(len(df[df.closeDev >= percentMove]) / len(df))
        
    elif criteria == 'closeUnder':
        criteriaMet = (len(df[df.closeDev <= percentMove]) / len(df))
        
    elif criteria == 'hit':
        if percentMove >= 0:
            criteriaMet = (len(df[df.highDev >= percentMove]) / len(df))
        else:
            criteriaMet = (len(df[df.highDev <= percentMove]) / len(df))
    elif criteria == 'notHit':
        if percentMove >= 0:
            criteriaMet = (len(df[df.highDev <= percentMove]) / len(df))
        else:
            criteriaMet = (len(df[df.highDev >= percentMove]) / len(df))
    elif criteria == 'closeBetween':
        if percentMove >=0:
            df2 = df[df.closeDev < percentMove]
            criteriaMet = len(df2[df2.closeDev > percentMove2]) / len(df)
        else:
            df2 = df[df.closeDev > percentMove]
            criteriaMet = len(df2[df2.closeDev < percentMove2]) / len(df)
    elif criteria == 'hitOne':
        true = 0
        n = 0
        while n < len(df):
            if percentMove > 0:
                if df.highDev.iloc[n] >= percentMove:
                    true += 1
                    n += 1
                elif df.lowDev.iloc[n] <= percentMove2:
                    true += 1
                    n+=1
                else:
                    n+=1
            else:
                if df.lowDev.iloc[n] <= percentMove:
                    true+=1
                    n+=1
                elif df.highDev.iloc[n] >= percentMove2:
                    true+=1
                    n+=1
                else:
                    n+=1
        criteriaMet = true / len(df)
    elif criteria == 'hitBoth':
        true = 0
        n = 0
        while n < len(df):
            if percentMove > 0:
                if df.highDev.iloc[n] >= percentMove and df.lowDev.iloc[n] <= percentMove2:
                    true +=1
                    n +=1
                else:
                    n+=1
                    
            elif df.lowDev.iloc[n] <= percentMove and df.highDev.iloc[n] >= percentMove2:
                true += 1
                n+=1
            else:
               n+=1
        criteriaMet = true / len(df)
    elif crieria == 'hitNeither':
        if percentMove >=0:
            df2 = df[df.highDev < percentMove]
            criteriaMet = len(df2[df2.lowDev > percentMove2]) / len(df)
        else:
            df2 = df[df.lowDev > percentMove]
            criteriaMet = len(df2[df2.highDev < percentMove2]) / len(df)
    else:
        criteriaMet = False

    if criteriaMet == False:
        print('Invalid Criteria. Options: hit,notHit, closeOver, '+
              'CloseUnder, closeBetween, oneHit, bothHit, hitNeither')
    else:
        winChane = criteriaMet
        loseChance = 1-criteriaMet
        if chanceOnly == False: 
            riskAdjReturns = (criteriaMet * win) + ((1-criteriaMet)*loss)
            print('% Win, % Loss, RaR')
            rarOutput = [winChance, loseChance, riskAdjReturns]
        else:
            print('% Win, % Loss')
            rarOutput = [winChance, loseChance]
        

    return rarOutput

def highLowRange(df):
    df['highLowRange'] = df.High - df.Low
    df['highLowRange%'] = df.highLowRange / df.Open

    return df

def closeToOpen(df):
    df['closeToOpen'] = df.Open - df.Close.shift(-1)
    df['closeToOpenPercent'] = df.closeToOpen / df.Close.shift(-1)
    df.drop(df.index[len(df)-1])

    return df

def overNightRisk(df, filterSet = False):
    df = closeToOpen(df)

    if filterSet == False:
        filterSet = [-.05, -.045, -.04, -.035, -.03, -.025, -.02, -.0175, -.015, -.0125, -.01,
                -.009, -.008, -.007, -.006, -.005, -.004, -.003, -.002, -.001, 0, .001,
                .002, .003, .004, .005, .006, .007, .008, .009, .01, .0125, .015, .0175, .02,
                .025, .03, .035, .04, .045, .05]
    else:
        pass

    overNightRisk = pd.DataFrame(index = filterSet)

    overNightRiskChance = []

    for x in filterSet:
        if x < 0:
            overNightRiskChance.append(len(df[df.closeToOpenPercent < x]) / len(df))
        elif x == 0:
            overNightRiskChance.append((len(df) - (len(df[df.closeToOpenPercent < -.001]) + len(df[df.closeToOpenPercent > .001]))) / len(df))
        else:
            overNightRiskChance.append(len(df[df.closeToOpenPercent > x]) / len(df))
    overNightRisk['overNightRiskChance'] = overNightRiskChance
    print(df.closeToOpenPercent.describe())
    return overNightRisk

                        
        
    


          
        
        
    
    
                    
            
        
        
    
    






