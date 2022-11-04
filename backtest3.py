# region imports
from AlgorithmImports import *
from Selection.OptionUniverseSelectionModel import OptionUniverseSelectionModel
import datetime
from QuantConnect.Securities.Option import OptionPriceModels
# endregion

class USEquityOptionsDataAlgorithm(QCAlgorithm):
    def UniverseFunc(self, universe):
        # include weekly contracts
        return universe.IncludeWeeklys().Expiration(TimeSpan.FromDays(0), TimeSpan.FromDays(600)).Strikes(-300,300)

    def Initialize(self) -> None:
        self.SetStartDate(2022, 6, 14)
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
        #option.SetFilter(-300, 300, 0, 600)
        option.SetFilter(self.UniverseFunc)

        self.contract = None
        self.short_contract = None
        self.total_premium_collected = 0
        self.last_roll_date = None
        self.num_long_calls = 0
        self.num_short_calls = 0

    def OnData(self, slice: Slice) -> None:

        if self.IsWarmingUp: return
        if self.Time.hour < 14 and self.contract is not None and self.short_contract is not None: return 
        if self.short_contract is not None:
            self.Debug(f"{self.short_contract.AskPrice} premium")
        chain = slice.OptionChains.get(self.option_symbol)
        if chain:
            # Select call contracts
            calls = [contract for contract in chain if contract.Right == OptionRight.Call]
            if len(calls) == 0:
                return
            # Select the call contracts with the furthest expiration
            furthest_expiry = sorted(calls, key = lambda x: x.Expiry, reverse=True)[0].Expiry
            #next_week_expiry = sorted(calls[:], key = lambda x: x.Expiry, reverse=False)[3].Expiry

            #self.Debug(f"exps: {sorted(list(set([i.Expiry for i in calls[:]])), reverse=False)}")
            next_week_expiry = sorted(list(set([i.Expiry for i in calls[:]])), reverse=False)[3]
            #self.Debug(f"next_week_expiry: {next_week_expiry}")
            # no need to roll
            if self.contract is not None:
                if furthest_expiry != self.contract.Expiry:
                    furthest_expiry_calls = [contract for contract in calls if contract.Expiry == furthest_expiry]
                    # From the remaining contracts, select the one with its strike closest to the underlying price
                    chosen_call = sorted(furthest_expiry_calls, key = lambda x: abs(x.Greeks.Delta - 90))[0]
                    #self.contract = chosen_call

                    self.MarketOrder(self.contract.Symbol, -1)
                    self.num_long_calls -= 1
                    self.Debug(f"rolling long call")

                    #self.contract = sorted(furthest_expiry_calls, key = lambda x: abs(chain.Underlying.Price - x.Strike))[0]
                    self.MarketOrder(chosen_call.Symbol, 1)
                    self.num_long_calls += 1
                    self.contract = chosen_call
                    self.Debug(f"Long Contract Updated: {self.contract.Greeks.Delta} delta, {self.contract.Expiry}")
            else:
                furthest_expiry_calls = [contract for contract in calls if contract.Expiry == furthest_expiry]
                # From the remaining contracts, select the one with its strike closest to the underlying price
                chosen_call = sorted(furthest_expiry_calls, key = lambda x: abs(x.Greeks.Delta - .9))[0]
                #self.contract = chosen_call
                #self.contract = sorted(furthest_expiry_calls, key = lambda x: abs(chain.Underlying.Price - x.Strike))[0]
                self.MarketOrder(chosen_call.Symbol, 1)
                self.num_long_calls += 1
                self.contract = chosen_call
                self.Debug(f"Long Contract Updated: {self.contract.Greeks.Delta} delta, {self.contract.Expiry}")


            #for short contract
            #roll short call every friday
            #if call is in the money close entire position and reopen
            #sell 16 delta calls
            if self.short_contract is not None:
                #short call expiring today
                if self.Time.date() == self.short_contract.Expiry.date():
                    #last hour of trading
                    if self.Time.hour >= 14 and (self.last_roll_date is None or (self.last_roll_date is not None and self.last_roll_date != self.Time.date())):
                        if chain.Underlying.Price > self.short_contract.Strike:
                            #close entire position because it's in the money
                            short_expiry_calls = [contract for contract in calls if contract.Expiry == next_week_expiry]
                            # From the remaining contracts, select the one with its strike closest to the underlying price
                            chosen_short_call = sorted(short_expiry_calls, key = lambda x: abs(x.Greeks.Delta - .16))[0]
                            #self.contract = chosen_call

                            self.MarketOrder(self.short_contract.Symbol, 1)
                            self.num_short_calls -= 1
                            self.MarketOrder(self.contract.Symbol, -1)
                            self.num_long_calls -= 1
                            self.Debug(f"closing position")
                            self.Debug(f"total premium collected: {self.total_premium_collected}")
                            self.Debug(f"{self.num_short_calls} short, {self.num_long_calls} long")
                            self.short_contract = None
                            self.contract = None

                        else:
                            #roll short call
                            short_expiry_calls = [contract for contract in calls if contract.Expiry == next_week_expiry]
                            # From the remaining contracts, select the one with its strike closest to the underlying price
                            chosen_short_call = sorted(short_expiry_calls, key = lambda x: abs(x.Greeks.Delta - .16))[0]
                            #self.contract = chosen_call

                            self.MarketOrder(self.short_contract.Symbol, 1)
                            self.num_short_calls -= 1
                            
                            prev_call = list(filter(lambda x: x.Expiry == self.short_contract.Expiry and x.Strike == self.short_contract.Strike, calls))[0]
                            
                            self.total_premium_collected -= prev_call.BidPrice
                            self.Debug(f"rolling short call, buying back: for {prev_call.BidPrice} premium")

                            self.MarketOrder(chosen_short_call.Symbol, -1)
                            self.num_short_calls += 1
                            self.short_contract = chosen_short_call
                            self.total_premium_collected += self.short_contract.AskPrice
                            self.last_roll_date = self.Time.date()
                            self.Debug(f"Short Contract Updated: {self.short_contract.Greeks.Delta} delta, {self.short_contract.AskPrice} premium, {self.short_contract.Expiry}")
                            self.Debug(f"total premium collected: {self.total_premium_collected}")
                            self.Debug(f"{self.num_short_calls} short, {self.num_long_calls} long")
            else:
                short_expiry_calls = [contract for contract in calls if contract.Expiry == next_week_expiry]
                chosen_short_call = sorted(short_expiry_calls, key = lambda x: abs(x.Greeks.Delta - .16))[0]
                self.MarketOrder(chosen_short_call.Symbol, -1)
                self.num_short_calls += 1
                self.short_contract = chosen_short_call
                self.total_premium_collected += self.short_contract.AskPrice
                self.Debug(f"Short Contract Updated: {self.short_contract.Greeks.Delta} delta, {self.short_contract.AskPrice} premium, {self.short_contract.Expiry}")
                self.Debug(f"total premium collected: {self.total_premium_collected}")
          
    def OnSecuritiesChanged(self, changes: SecurityChanges) -> None:
        for security in changes.AddedSecurities:
            # Historical data
            history = self.History(security.Symbol, 10, Resolution.Hour)
            #self.Debug(f"We got {len(history)} from our history request for {security.Symbol}")