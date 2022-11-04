# region imports
from AlgorithmImports import *
from Selection.OptionUniverseSelectionModel import OptionUniverseSelectionModel
import datetime
from QuantConnect.Securities.Option import OptionPriceModels
# endregion

class USEquityOptionsDataAlgorithm(QCAlgorithm):
    def Initialize(self) -> None:
        self.SetStartDate(2022, 10, 14)
        self.SetEndDate(2022, 10, 21)
        self.SetCash(100000)

        # Requesting data
        self.underlying = self.AddEquity("SPY").Symbol
        option = self.AddOption("SPY")
        option.PriceModel = OptionPriceModels.CrankNicolsonFD()
        self.option_symbol = option.Symbol
        #for greeks
        self.SetWarmUp(30, Resolution.Daily)
        # Set our strike/expiry filter for this option chain
        option.SetFilter(-300, 300, 100, 500)
        
        self.contract = None
        self.short_contract = None

    def OnData(self, slice: Slice) -> None:

        if self.IsWarmingUp: return
        chain = slice.OptionChains.get(self.option_symbol)
        if chain:
            # Select call contracts
            calls = [contract for contract in chain if contract.Right == OptionRight.Call]
            if len(calls) == 0:
                return
            # Select the call contracts with the furthest expiration
            furthest_expiry = sorted(calls, key = lambda x: x.Expiry, reverse=True)[0].Expiry
            next_week_expiry = sorted(calls, key = lambda x: x.Expiry, reverse=False)[3].Expiry

            # no need to roll
            if self.contract is not None:
                if furthest_expiry == self.contract.Expiry:
                    return
                else:
                    furthest_expiry_calls = [contract for contract in calls if contract.Expiry == furthest_expiry]
                    # From the remaining contracts, select the one with its strike closest to the underlying price
                    chosen_call = sorted(furthest_expiry_calls, key = lambda x: abs(x.Greeks.Delta - 90))[0]
                    #self.contract = chosen_call

                    self.MarketOrder(self.contract.Symbol, -1)
                    self.Debug(f"rolling long call")

                    #self.contract = sorted(furthest_expiry_calls, key = lambda x: abs(chain.Underlying.Price - x.Strike))[0]
                    self.MarketOrder(chosen_call.Symbol, 1)
                    self.contract = chosen_call
                    self.Debug(f"Contract Updated")
            else:
                furthest_expiry_calls = [contract for contract in calls if contract.Expiry == furthest_expiry]
                # From the remaining contracts, select the one with its strike closest to the underlying price
                chosen_call = sorted(furthest_expiry_calls, key = lambda x: abs(x.Greeks.Delta - .9))[0]
                #self.contract = chosen_call
                #self.contract = sorted(furthest_expiry_calls, key = lambda x: abs(chain.Underlying.Price - x.Strike))[0]
                self.MarketOrder(chosen_call.Symbol, 1)
                self.contract = chosen_call
                self.Debug(f"Contract Updated")

            #for short contract
            #roll short call every friday
            #if call is in the money close entire position and reopen
            #sell 16 delta calls
            if self.short_contract is not None:
                #short call expiring today
                if self.Time.date() == self.short_contract.Expiry:
                    return
                else:
                    short_expiry_calls = [contract for contract in calls if contract.Expiry == next_week_expiry]
                    # From the remaining contracts, select the one with its strike closest to the underlying price
                    chosen_short_call = sorted(short_expiry_calls, key = lambda x: abs(x.Greeks.Delta - .16))[0]
                    #self.contract = chosen_call

                    self.MarketOrder(self.contract.Symbol, -1)
                    self.Debug(f"rolling short call")

                    self.MarketOrder(chosen_call.Symbol, 1)
                    self.short_contract = chosen_short_call
                    self.Debug(f"Contract Updated")
            else:
                short_expiry_calls = [contract for contract in calls if contract.Expiry == furthest_expiry]
                # From the remaining contracts, select the one with its strike closest to the underlying price
                chosen_call = sorted(short_expiry_calls, key = lambda x: abs(x.Greeks.Delta - 90))[0]
                self.MarketOrder(chosen_call.Symbol, 1)
                self.short_contract = chosen_call
                self.Debug(f"Contract Updated")


            '''

            furthest_expiry_calls = [contract for contract in calls if contract.Expiry == furthest_expiry]

            # From the remaining contracts, select the one with its strike closest to the underlying price
            strike_call = None
            chosen_call = sorted(furthest_expiry_calls, key = lambda x: abs(x.Greeks.Delta - 90))[0]
            #self.contract = chosen_call

            if self.contract is not None:
                self.MarketOrder(self.contract.Symbol, -1)
                self.Debug(f"rolling long call")


            #self.contract = sorted(furthest_expiry_calls, key = lambda x: abs(chain.Underlying.Price - x.Strike))[0]
            self.MarketOrder(chosen_call.Symbol, 1)
            self.contract = chosen_call
            self.Debug(f"Contract Updated")'''

                
                
    def OnSecuritiesChanged(self, changes: SecurityChanges) -> None:
        
        for security in changes.AddedSecurities:
            # Historical data
            history = self.History(security.Symbol, 10, Resolution.Daily)
            #self.Debug(f"We got {len(history)} from our history request for {security.Symbol}")