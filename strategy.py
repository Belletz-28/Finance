import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import seaborn as sns

def unprofitable (commission, *args, **kwargs): 
    if args[0]< commission and args[1] < commission:
            raise ValueError("Unprofitable, change upper and lower limit")
    else: 
        print("Estimated profit for grid")
        print(f'upper profit: {args[0]} lower profit: {args[1]}')

def evaluateGrid(close, grids, prices):
    result = False
    for i in range(grids - 1 ):
        if close >= prices[i] and close <= prices[i+1]:
            result = (prices[i],prices[i+1])
    return result

def gridTrading(pair="BTC-EUR", upperLimit=40000, lowerLimit=30000, gridType="Aritmetic",grids=10,capital=10000,upperTrigger=60000, lowerTrigger=30000, onClose = "Cancel", commission = 0.1):
    data = pd.read_parquet(f'../Datasets/Binance/{pair}.parquet')
    data.drop(["quote_asset_volume","number_of_trades","taker_buy_base_asset_volume","taker_buy_quote_asset_volume"], axis=1, inplace= True)
    day = 60 * 24
    month =  data.tail(day)
    if gridType == "Aritmetic":
        # same price for all grids
        gridCapital = capital / grids
        priceDiff = (upperLimit - lowerLimit)/grids
        prices = []
        operativityArray = list()
        for grid in range(grids):
            # + 1 for arrays
            price = lowerLimit + priceDiff * (grid + 1)
            prices.append(price)
            operativityArray.append({"price":price, "operation":"buy"})
        print(prices)
        upperLimitProfit = (1 + (upperLimit - lowerLimit) / (grids* upperLimit)) * (1 - commission) * 2 - 1
        lowerLimitProfit = (1 + (upperLimit - lowerLimit) / (grids* lowerLimit)) * (1 - commission) * 2 - 1
        unprofitable(commission, upperLimitProfit, lowerLimitProfit)
        strategy = pd.DataFrame(columns=["Row","Date","Entry","Exit","Trade","Qty","Type","ROI","Capital","GridLevel"],dtype = 'float')
        strategy.set_index("Row", inplace = True)
        print(strategy.dtypes)
        print(operativityArray)
        for index, row in month.iterrows():
            if row["high"] < upperLimit and row["low"] > lowerLimit and capital > (capital * commission * 2):
                priceRange = evaluateGrid(row["close"], grids, prices)
                print(priceRange, row["close"])
                if strategy.empty == True and priceRange != False:
                    capital += - gridCapital - ( capital * commission * 2 / 100) 
                    strategy.loc[len(strategy.index)] = [dt.datetime.today(),row["close"],None,None, gridCapital / row["close"],"buy",None,capital,priceRange]
                elif not strategy.empty and priceRange != False:
                    if row["close"] < priceRange[1] and row["close"] > priceRange[0] : 
                        capital += -1000
                        print("Operating")
                        pass
                    else:
                        print("Waiting...")  
            else:
                print("Not in grid range - Capital:" + str(capital))
                print("Closing all operations...")
                print("Done")
                break
        print(strategy)
    elif gridType == "Geometric":
        # % price for all grids
        priceRatio = (upperLimit / lowerLimit) ** (1/grids)
        diffRatio = ( priceRatio - 1) * 100
        print(diffRatio)
        for grid in range(grids):
            # + 1 for arrays
            prices = lowerLimit * priceRatio ** (grid + 1)
            print(prices)
        gridProfit = priceRatio -1 -2 ** commission
        unprofitable(commission,gridProfit,gridProfit)
    else:
        print("Invalid Grid Type")
        raise ValueError

if __name__ == '__main__': 
    gridTrading(gridType= "Aritmetic")
            
