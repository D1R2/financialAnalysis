#Custom Finance Data Analysis Package

import pandas as pd
import numpy as np

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

def correlationNew(df1, df2, ticker1, ticker2, col, toCol):
    #Takes col to toCol movement (ie. Open to Close).
    #Assumes data is Newest to Oldest going down the list. 
    #Continued Improvement Notes: Improve data output format.
        #Add note detailing what this actually does. 
    

    tickers = [[df1, ticker1], [df2, ticker2]]

    df = pd.DataFrame()

    for dataFrame, ticker in tickers:
        dfTicker = dataFrame
        #Ticker1 PercentChange = col 7, ABS Ticker1 = col 8, Ticker2 PercentChange = col 9, ABS Percent Change = col 10
        if col == toCol:
            df['{} PercentChange'.format(ticker)] = (dfTicker[toCol] - dfTicker[col].shift(-1)) / dfTicker[col].shift(-1)
        else:
            df['{} PercentChange'.format(ticker)] = (dfTicker[toCol] - dfTicker[col]) / dfTicker[col]
        df['{} AbsPercentChange'.format(ticker)] = abs(df['{} PercentChange'.format(ticker)])
        
    df.dropna(inplace = True)
    dfLength = len(df)

    
    t1Up = df[df.iloc[:,0]>0]
    bothUp = t1Up[t1Up.iloc[:,2]>0]
    bothUp['{} Change - {} Change'.format(ticker2, ticker1)] = bothUp.iloc[:,2] - bothUp.iloc[:,0] #bothUp.iloc[:,4]
    
    t1Down = df[df.iloc[:,0]<0]
    bothDown = t1Down[t1Down.iloc[:,2]<0]
    bothDown['{} Change - {} Change'.format(ticker2, ticker1)] = bothDown.iloc[:,2] - bothDown.iloc[:,0] #bothDown.iloc[:,4]

    df['AbsValueDif'] = df.iloc[:,3] - df.iloc[:,1] #Checks if abs val move of T2 is greater than T1. 

    upCor = len(bothUp) / len(t1Up)
    upCorData = len(bothUp)
    upCorMeanDif = bothUp.iloc[:,4].mean()
    upCorStdDif = bothUp.iloc[:,4].std()
    
    downCor  = len(bothDown) / len(t1Down)
    downCorData = len(bothDown)
    downCorMeanDif = bothDown.iloc[:,4].mean()
    downCorStdDif = bothDown.iloc[:,4].std()

    dirCor = upCor + downCor
    dirCorData = upCorData + downCorData

    absGreater = len(df[df['AbsValueDif']>0]) / len(df)
    absLess = 1 - absGreater
    absDifMean = df['AbsValueDif'].mean()

    dfReturned = pd.DataFrame({'Ticker1 vs Ticker2': '{} vs {}'.format(ticker1, ticker2),
                      'Col to Col': ['{} to {}'.format(col, toCol)],
                      'dfLength': [dfLength],
                      'upCor':[upCor],
                      'upCorData':[upCorData],
                      'upCorMeanDif':[upCorMeanDif],
                      'upCorStdDif':[upCorStdDif],
                      'downCor':[downCor],
                      'downCorData':[downCorData],
                      'downCorMeanDif':[downCorMeanDif],
                      'downCorStdDif':[downCorStdDif],
                      'absGreater':[absGreater],
                      'absLess':[absLess],
                      'absDifMean':[absDifMean]})
    return dfReturned

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
