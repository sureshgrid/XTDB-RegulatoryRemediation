# ************************************************************************
# Author           : Suresh Nageswaran suresh@griddynamics.com
# File Name        : generator.py
# Description      : The Data Generator class spits out JSON to an output file
# Revision History :
# Date            Author            Comments
# 
# ************************************************************************
# generator.py

import random
import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
from decimal import Decimal

# class BitemporalDataGenerator:
#     """
#     Generates bitemporal data for trades and counterparties with realistic financial patterns.
#     Supports temporal validity tracking and sophisticated market manipulation patterns.
#     """
#     def __init__(self, start_date: str, end_date: str, config: Dict[str, Any] = None):
#         """
#         Initialize the generator with a date range and reference data.
        
#         The generator maintains internal state of active counterparties and their
#         relationships to ensure consistent data generation across the time period.
#         """

#         # First let's define a registry of required methods
#         self._required_methods = {
#             # These are my core generation methods ->
#             'generate_dataset': self.generate_dataset,
#             'generate_trade': self.generate_trade,
#             'generate_trade_correction': self.generate_trade_correction,
            
#             # Internal helper functions
#             '_generate_initial_traders': self._generate_initial_traders,
#             '_generate_initial_counterparties': self._generate_initial_counterparties,
#             '_initialize_trading_relationships': self._initialize_trading_relationships,
#             '_generate_price': self._generate_price
            
            
#             # Scenario support methods are in scenarios.py
#             #'generate_wash_trade_pair': self.generate_wash_trade_pair,
#             #'get_layering_counterparty': self.get_layering_counterparty,
#             #'generate_spoofing_order': self.generate_spoofing_order
#         }

#         # Core reference data               
#         self.start_date = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S.%fZ')        
#         self.end_date = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S.%fZ')
        
#         self.config = config or {}
#         # Read in the securities list into a dict obj
#         securities_list = self.config.get("securities", [])
#         # If nothing's there, this is the default set we assume
#         if not securities_list:
#              self.securities = {
#              'AAPL': {'base_price': Decimal('150.0'), 'volatility': 0.02},
#              'GOOGL': {'base_price': Decimal('2800.0'), 'volatility': 0.025},
#              'MSFT': {'base_price': Decimal('300.0'), 'volatility': 0.018},
#              'AMZN': {'base_price': Decimal('3300.0'), 'volatility': 0.03}
#          }

#         # Otherwise use the configured values and read them into the dict obj
#         self.securities = {}
#         for sec in securities_list:
#                 ticker = sec["ticker"]
#                 # Keep volatility as float, convert only base_price to Decimal        
#                 base_price = Decimal(sec["base_price"])
#                 volatility = sec["volatility"]
#                 self.securities[ticker] = {
#                     "base_price": base_price,
#                     "volatility": volatility
#                 }
        
#         # Initialize core entities
#         self.traders = self._generate_initial_traders()
#         self.counterparties = self._generate_initial_counterparties()
#         self.trading_relationships = self._initialize_trading_relationships()

#     def _generate_initial_traders(self) -> List[Dict[str, Any]]:
#         """
#         Generate a set of traders with realistic attributes.
#         Each trader has specific permissions and trading capabilities.
#         """
#         trader_types = [
#             ("Cash Equity", "Market Making"),
#             ("Program Trading", "Customer Facilitation"),
#             ("Equity Cash", "Principal Trading"),
#             ("Delta One", "ETF Market Making"),
#             ("Portfolio Trading", "Agency Trading")
#         ]
        
#         traders = []
#         for i in range(1, 6):
#             trader_id = f"TR{str(i).zfill(3)}"
#             desk, strategy = random.choice(trader_types)
#             traders.append({
#                 "trader_id": trader_id,
#                 "desk": desk,
#                 "strategy_type": strategy,
#                 "algo_enabled": random.choice([True, False]),
#                 "permissions": {
#                     "max_order_size": random.randint(10000, 100000),
#                     "markets": random.sample(list(self.securities.keys()), 
#                                           random.randint(2, len(self.securities))),
#                     "risk_limit": random.randint(1000000, 5000000)
#                 }
#             })
#         return traders

#     def _generate_initial_counterparties(self) -> List[Dict[str, Any]]:
#         """
#         Generate counterparty entities with complete regulatory and business attributes.
#         Includes all necessary fields for compliance reporting and risk management.
#         """
#         account_types = {
#             'R': 'Retail',
#             'I': 'Institution',
#             'M': 'Market Maker',
#             'B': 'Broker-Dealer',
#             'P': 'Proprietary'
#         }
        
#         risk_ratings = {
#             'A': {'trading_limit': 5000000, 'probability': 0.4},
#             'B': {'trading_limit': 2000000, 'probability': 0.3},
#             'C': {'trading_limit': 1000000, 'probability': 0.2},
#             'D': {'trading_limit': 500000, 'probability': 0.1}
#         }
        
#         counterparties = []
#         for i in range(1, 10):  # Increased number for more realistic patterns
#             cp_id = f"CP{str(i).zfill(3)}"
            
#             # Select risk rating based on probability distribution
#             risk_rating = random.choices(
#                 list(risk_ratings.keys()),
#                 weights=[r['probability'] for r in risk_ratings.values()]
#             )[0]
            
#             counterparties.append({
#                 "_id": cp_id,
#                 "type": "counterparty",
#                 # Regulatory identifiers
#                 "executing_broker_id": f"EXEC{i}",
#                 "clearing_broker_id": f"CLR{i}",
#                 "clearing_account": f"CA{str(i).zfill(6)}",
#                 "correspondent_id": f"CORR{i}",
#                 "beneficial_owner_id": f"BO{str(i).zfill(6)}",
                
#                 # Business attributes
#                 "account_type": random.choice(list(account_types.keys())),
#                 "account_category": account_types[random.choice(list(account_types.keys()))],
#                 "status": "active",
#                 "risk_rating": risk_rating,
#                 "trading_limit": risk_ratings[risk_rating]['trading_limit'],
                
#                 # Temporal validity
#                 "_valid_from": self.start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
#                 "_valid_to": None,
#                 "cp_update_sequence": 1,
                
#                 # Additional metadata
#                 "credit_status": random.choice(["Approved", "Watch", "Restricted"]),
#                 "margin_requirement": random.randint(25, 100),
#                 "settlement_instructions": {
#                     "default_currency": "USD",
#                     "settlement_method": random.choice(["DTC", "FED", "SWIFT"]),
#                     "settlement_cycle": "T+2"
#                 }
#             })
#         return counterparties

#     def _initialize_trading_relationships(self) -> Dict[str, List[str]]:
#         """
#         Initialize trading relationships between counterparties.
#         This helps in generating realistic trading patterns and detecting wash trades.
        
#         Returns:
#             Dict[str, List[str]]: A dictionary mapping counterparty IDs to lists of their trading partners
        
#         Raises:
#             ValueError: If there are not enough counterparties to establish relationships
#         """
#         if len(self.counterparties) < 2:
#             raise ValueError("Need at least 2 counterparties to establish trading relationships")
            
#         relationships = {}
#         for cp in self.counterparties:
#             # Each counterparty can trade with 2-4 other counterparties, but limited by available counterparties
#             max_partners = min(4, len(self.counterparties) - 1)
#             min_partners = min(2, max_partners)
            
#             other_counterparties = [c["_id"] for c in self.counterparties if c["_id"] != cp["_id"]]
#             num_partners = random.randint(min_partners, max_partners)
            
#             trading_partners = random.sample(other_counterparties, num_partners)
#             relationships[cp["_id"]] = trading_partners
        
#         return relationships

#     def _generate_price(self, security: str, is_suspicious: bool = False) -> Decimal:
#         """
#         Generate a realistic price for a security with optional suspicious variation.
#         Takes into account base price and volatility factors.
#         """
#         base_price = float(self.securities[security]['base_price']) #typecasting to float what a pain
#         volatility = self.securities[security]['volatility']
        
#         if is_suspicious:
#             # Suspicious prices have higher volatility and potential manipulation patterns
#             variation = random.uniform(0.15, 0.25)
#             direction = 1 if random.random() > 0.5 else -1
#         else:
#             variation = random.uniform(-volatility, volatility)
#             direction = 1
        
#         #price = base_price * (1 + (direction * variation))
#         price = base_price * 1 + (direction * variation)
#         return Decimal(str(round(price, 2))) # typecasting again, why dear Lord Ganesha, why

#     def generate_trade(
#         self,
#         trade_date: datetime,
#         is_suspicious: bool = False,
#         scenario_type: Optional[str] = None,
#         counterparty_id: Optional[str] = None,
#         side: Optional[str] = None
#     ) -> Dict[str, Any]:
#         """
#         Generate a single trade with complete execution and counterparty details.
#         Supports various market manipulation scenarios and temporal tracking.
#         """
#         trade_id = str(uuid.uuid4())
#         security = random.choice(list(self.securities.keys()))
        
#         # Select or use provided counterparty
#         cp = (next((cp for cp in self.counterparties if cp["_id"] == counterparty_id), None)
#               if counterparty_id else random.choice(self.counterparties))
        
#         trader = random.choice(self.traders)
#         trade_side = side if side else random.choice(["B", "S"])
        
#         return {
#             # Core trade identifiers
#             "_id": trade_id,
#             "type": "trade",
#             "scenario_type": scenario_type,
            
#             # Execution details
#             "execution_timestamp": trade_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
#             "symbol": security,
#             "price": self._generate_price(security, is_suspicious),
#             "quantity": random.randint(100, 1000),
#             "side": trade_side,
            
#             # Counterparty information
#             "executing_broker_id": cp["executing_broker_id"],
#             "executing_trader_id": trader["trader_id"],
#             "clearing_broker_id": cp["clearing_broker_id"],
#             "clearing_account": cp["clearing_account"],
#             "beneficial_owner_id": cp["beneficial_owner_id"],
#             "account_type": cp["account_type"],
#             "counterparty_id": cp["_id"],
            
#             # Temporal validity
#             "_valid_from": trade_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
#             "_valid_to": None,
            
#             # Additional execution attributes
#             "trade_report_time": (trade_date + timedelta(seconds=random.randint(1, 5)))
#                                 .strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
#             "settlement_date": (trade_date + timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
#             "trade_status": "executed",
#             "execution_venue": random.choice(["NYSE", "NASDAQ", "ARCA", "BATS"]),
#             "execution_capacity": cp["account_type"],
#             "algo_id": trader["trader_id"] if trader["algo_enabled"] else None
#         }

#     def generate_trade_correction(
#         self,
#         trade: Dict[str, Any],
#         correction_date: datetime
#     ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
#         """
#         Generate a correction for an existing trade, maintaining temporal validity.
#         Returns both the original trade (with updated validity) and the correction.
#         """
#         # End validity of original trade
#         original_trade = trade.copy()
#         original_trade["_valid_to"] = correction_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
#         # Create correction with same ID but new validity period
#         corrected_trade = trade.copy()
#         corrected_trade.update({
#             #"price": round(trade["price"] * (1 + random.uniform(-0.02, 0.02)), 2),
#             "price": Decimal(str(float(trade["price"]) * (1 + random.uniform(-0.02, 0.02)))),
#             "trade_status": "corrected",
#             "correction_reason": random.choice([
#                 "Price adjustment",
#                 "Quantity revision",
#                 "Settlement instruction update",
#                 "Counterparty correction"
#             ]),
#             "_valid_from": correction_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
#             "_valid_to": None
#         })
        
#         return original_trade, corrected_trade

#     def generate_dataset(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
#         """
#         Generate complete datasets for both counterparties and trades.
#         Includes temporal changes and relationships between entities.
#         """
#         counterparty_documents = []
#         trade_documents = []
        
#         # Include initial counterparty states
#         counterparty_documents.extend(self.counterparties)
        
#         current_date = self.start_date
#         while current_date <= self.end_date:
#             # Generate daily trading activity
#             #for _ in range(random.randint(30, 700)):
#             for _ in range(random.randint(10, 25)):
#                 trade_time = current_date + timedelta(minutes=random.randint(0, 1440))
#                 trade = self.generate_trade(
#                     trade_time,
#                     is_suspicious=(random.random() < 0.15)
#                 )
#                 trade_documents.append(trade)
                
#                 # Potentially generate corrections
#                 if random.random() < 0.1:
#                     correction_date = trade_time + timedelta(days=random.randint(1, 2))
#                     if correction_date <= self.end_date:
#                         original, correction = self.generate_trade_correction(
#                             trade, correction_date
#                         )
#                         trade_documents.extend([original, correction])
            
#             # Generate counterparty changes
#             if random.random() < 0.1:
#                 cp = random.choice(self.counterparties)
#                 original, updated = self.generate_counterparty_change(cp, current_date)
#                 counterparty_documents.extend([original, updated])
#                 self.counterparties.remove(cp)
#                 self.counterparties.append(updated)
            
#             current_date += timedelta(days=1)
        
#         return trade_documents, counterparty_documents

#     def generate_counterparty_change(
#         self,
#         counterparty: Dict[str, Any],
#         change_date: datetime
#     ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
#         """
#         Generate changes to counterparty attributes while maintaining temporal validity.
#         Models realistic business changes like risk rating updates or status changes.
#         """
#         original_cp = counterparty.copy()
#         original_cp["_valid_to"] = change_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
#         updated_cp = counterparty.copy()
#         updated_cp.update({
#             "status": random.choice(["active", "suspended", "restricted"]),
#             "risk_rating": random.choice(["A", "B", "C", "D"]),
#             "trading_limit": counterparty["trading_limit"] * random.uniform(0.5, 1.5),
#             "_valid_from": change_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
#             "_valid_to": None,
#             "cp_update_sequence": counterparty.get("cp_update_sequence", 1) + 1
#         })
        
#         return original_cp, updated_cp
    
#     import random
# import uuid
# from datetime import datetime, timedelta
# from typing import Dict, List, Any, Tuple, Optional
# from decimal import Decimal

# Configure logging with timestamp and log level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BitemporalDataGenerator:
    """
    Generates bitemporal data for trades and counterparties with realistic financial patterns.
    Supports temporal validity tracking and sophisticated market manipulation patterns.
    """
    def __init__(self, start_date: str, end_date: str, config: Dict[str, Any] = None):
        """
        Initialize the generator with a date range and reference data.
        
        The generator maintains internal state of active counterparties and their
        relationships to ensure consistent data generation across the time period.
        """
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')

        self.config = config or {}
        self.max_trades = self.config.get("generation", {}).get("trades_per_day")
        if not self.max_trades:
            self.max_trades = 25        

        logger.info(f"The number of trades being generated is: {self.max_trades}")
        

        # Build self.securities from config or fallback
        securities_list = self.config.get("securities", [])
        if not securities_list:
            self.securities = {
                'AAPL': {'base_price': Decimal('150.0'), 'volatility': 0.02},
                'GOOGL': {'base_price': Decimal('2800.0'), 'volatility': 0.025},
                'MSFT': {'base_price': Decimal('300.0'), 'volatility': 0.018},
                'AMZN': {'base_price': Decimal('3300.0'), 'volatility': 0.03}
            }
        else:
            self.securities = {}
            for sec in securities_list:
                ticker = sec["ticker"]
                base_price = Decimal(sec["base_price"])
                volatility = sec["volatility"]
                self.securities[ticker] = {
                    "base_price": base_price,
                    "volatility": volatility
                }

        self.traders = self._generate_initial_traders()
        self.counterparties = self._generate_initial_counterparties()
        self.trading_relationships = self._initialize_trading_relationships()

    def _generate_initial_traders(self) -> List[Dict[str, Any]]:
        trader_types = [
            ("Cash Equity", "Market Making"),
            ("Program Trading", "Customer Facilitation"),
            ("Equity Cash", "Principal Trading"),
            ("Delta One", "ETF Market Making"),
            ("Portfolio Trading", "Agency Trading")
        ]
        
        traders = []
        for i in range(1, 6):
            trader_id = f"TR{str(i).zfill(3)}"
            desk, strategy = random.choice(trader_types)
            traders.append({
                "trader_id": trader_id,
                "desk": desk,
                "strategy_type": strategy,
                "algo_enabled": random.choice([True, False]),
                "permissions": {
                    "max_order_size": random.randint(10000, 100000),
                    "markets": random.sample(list(self.securities.keys()), 
                                          random.randint(2, len(self.securities))),
                    "risk_limit": random.randint(1000000, 5000000)
                }
            })
        return traders

    def _generate_initial_counterparties(self) -> List[Dict[str, Any]]:
        """
        Generate counterparty entities with complete regulatory and business attributes,
        including valid timestamps for _valid_from/_valid_to recognized by XTDB 2.0.
        """
        account_types = {
            'R': 'Retail',
            'I': 'Institution',
            'M': 'Market Maker',
            'B': 'Broker-Dealer',
            'P': 'Proprietary'
        }
        
        risk_ratings = {
            'A': {'trading_limit': 5000000, 'probability': 0.4},
            'B': {'trading_limit': 2000000, 'probability': 0.3},
            'C': {'trading_limit': 1000000, 'probability': 0.2},
            'D': {'trading_limit': 500000, 'probability': 0.1}
        }
        
        counterparties = []
        for i in range(1, 10):
            cp_id = f"CP{str(i).zfill(3)}"
            risk_rating = random.choices(
                list(risk_ratings.keys()),
                weights=[r['probability'] for r in risk_ratings.values()]
            )[0]
            # Format the start date as a full timestamp with UTC suffix
            valid_from_str = self.start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            counterparties.append({
                "_id": cp_id,
                "type": "counterparty",
                "executing_broker_id": f"EXEC{i}",
                "clearing_broker_id": f"CLR{i}",
                "clearing_account": f"CA{str(i).zfill(6)}",
                "correspondent_id": f"CORR{i}",
                "beneficial_owner_id": f"BO{str(i).zfill(6)}",
                
                "account_type": random.choice(list(account_types.keys())),
                "account_category": account_types[random.choice(list(account_types.keys()))],
                "status": "active",
                "risk_rating": risk_rating,
                "trading_limit": risk_ratings[risk_rating]['trading_limit'],
                
                # Full timestamp string recognized by XTDB as TIMESTAMP WITH TIMEZONE
                "_valid_from": valid_from_str,
                "_valid_to": None,
                "cp_update_sequence": 1,
                
                "credit_status": random.choice(["Approved", "Watch", "Restricted"]),
                "margin_requirement": random.randint(25, 100),
                "settlement_instructions": {
                    "default_currency": "USD",
                    "settlement_method": random.choice(["DTC", "FED", "SWIFT"]),
                    "settlement_cycle": "T+2"
                }
            })
        return counterparties

    def _initialize_trading_relationships(self) -> Dict[str, List[str]]:
        if len(self.counterparties) < 2:
            raise ValueError("Need at least 2 counterparties to establish trading relationships")
            
        relationships = {}
        for cp in self.counterparties:
            max_partners = min(4, len(self.counterparties) - 1)
            min_partners = min(2, max_partners)
            other_counterparties = [c["_id"] for c in self.counterparties if c["_id"] != cp["_id"]]
            num_partners = random.randint(min_partners, max_partners)
            trading_partners = random.sample(other_counterparties, num_partners)
            relationships[cp["_id"]] = trading_partners
        return relationships

    def _generate_price(self, security: str, is_suspicious: bool = False) -> Decimal:
        base_price = float(self.securities[security]['base_price'])
        volatility = self.securities[security]['volatility']
        
        if is_suspicious:
            variation = random.uniform(0.15, 0.25)
            direction = 1 if random.random() > 0.5 else -1
        else:
            variation = random.uniform(-volatility, volatility)
            direction = 1
        
        price = base_price * 1 + (direction * variation)
        return Decimal(str(round(price, 2)))

    def generate_trade(
        self,
        trade_date: datetime,
        is_suspicious: bool = False,
        scenario_type: Optional[str] = "normal",
        counterparty_id: Optional[str] = None,
        side: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a single trade with complete execution and counterparty details,
        including full timestamp strings for all relevant fields.
        """
        trade_id = str(uuid.uuid4())
        security = random.choice(list(self.securities.keys()))
        
        cp = (next((cp for cp in self.counterparties if cp["_id"] == counterparty_id), None)
              if counterparty_id else random.choice(self.counterparties))
        
        trader = random.choice(self.traders)
        trade_side = side if side else random.choice(["B", "S"])
        
        # Format the main timestamp as a full UTC date-time
        exec_ts_str = trade_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        valid_from_str = exec_ts_str
        
        # trade_report_time is a few seconds after execution
        report_ts = trade_date + timedelta(seconds=random.randint(1, 5))
        report_ts_str = report_ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # settlement_date is okay as a plain date if you want
        settle_date = (trade_date + timedelta(days=2)).strftime('%Y-%m-%d')
        
        return {
            "_id": trade_id,
            "type": "trade",
            "scenario_type": scenario_type,
            
            "execution_timestamp": exec_ts_str,
            "symbol": security,
            "price": self._generate_price(security, is_suspicious),
            "quantity": random.randint(100, 1000),
            "side": trade_side,
            
            "executing_broker_id": cp["executing_broker_id"],
            "executing_trader_id": trader["trader_id"],
            "clearing_broker_id": cp["clearing_broker_id"],
            "clearing_account": cp["clearing_account"],
            "beneficial_owner_id": cp["beneficial_owner_id"],
            "account_type": cp["account_type"],
            "counterparty_id": cp["_id"],
            
            # For bitemporality
            "_valid_from": valid_from_str,
            "_valid_to": None,
            
            "trade_report_time": report_ts_str,
            "settlement_date": settle_date,
            "trade_status": "executed",
            "execution_venue": random.choice(["NYSE", "NASDAQ", "ARCA", "BATS"]),
            "execution_capacity": cp["account_type"],
            "algo_id": trader["trader_id"] if trader["algo_enabled"] else "NONE"
        }

    def generate_trade_correction(
        self,
        trade: Dict[str, Any],
        correction_date: datetime
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Generate a correction for an existing trade, maintaining temporal validity
        with updated timestamps that are recognized by XTDB 2.0 as TIMESTAMP WITH TIME ZONE.
        """
        # End validity of original
        original_trade = trade.copy()
        original_trade["_valid_to"] = correction_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        corrected_trade = trade.copy()
        corrected_trade.update({
            "price": Decimal(str(float(trade["price"]) * (1 + random.uniform(-0.02, 0.02)))),
            "trade_status": "corrected",
            "correction_reason": random.choice([
                "Price adjustment",
                "Quantity revision",
                "Settlement instruction update",
                "Counterparty correction"
            ]),
            "_valid_from": correction_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "_valid_to": None
        })
        
        return original_trade, corrected_trade

    def generate_dataset(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Generate complete datasets for trades and counterparties,
        ensuring timestamps are properly formatted for XTDB.
        """
        counterparty_documents = []
        trade_documents = []
        
        # start with the initial set
        counterparty_documents.extend(self.counterparties)
        
        current_date = self.start_date
        while current_date <= self.end_date:
            # Generate daily trading activity
            for _ in range(random.randint(10, self.max_trades)):
                trade_time = current_date + timedelta(minutes=random.randint(0, 1440))
                trade = self.generate_trade(trade_time, is_suspicious=(random.random() < 0.15))
                trade_documents.append(trade)
                
                # Potentially generate corrections
                if random.random() < 0.1:
                    correction_date = trade_time + timedelta(days=random.randint(1, 2))
                    if correction_date <= self.end_date:
                        original, correction = self.generate_trade_correction(trade, correction_date)
                        trade_documents.extend([original, correction])
            
            # Potentially generate a counterparty change
            if random.random() < 0.1:
                cp = random.choice(self.counterparties)
                original, updated = self.generate_counterparty_change(cp, current_date)
                counterparty_documents.extend([original, updated])
                self.counterparties.remove(cp)
                self.counterparties.append(updated)
            
            current_date += timedelta(days=1)
        
        return trade_documents, counterparty_documents

    def generate_counterparty_change(
        self,
        counterparty: Dict[str, Any],
        change_date: datetime
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Generate changes to counterparty attributes with valid timestamps
        recognized by XTDB.
        """
        original_cp = counterparty.copy()
        original_cp["_valid_to"] = change_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        updated_cp = counterparty.copy()
        updated_cp.update({
            "status": random.choice(["active", "suspended", "restricted"]),
            "risk_rating": random.choice(["A", "B", "C", "D"]),
            "trading_limit": counterparty["trading_limit"] * random.uniform(0.5, 1.5),
            "_valid_from": change_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "_valid_to": None,
            "cp_update_sequence": counterparty.get("cp_update_sequence", 1) + 1
        })
        
        return original_cp, updated_cp
