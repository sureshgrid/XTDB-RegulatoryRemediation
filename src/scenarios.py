# scenarios.py
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import random

from generator import BitemporalDataGenerator

def generate_layering_scenario(
    generator: BitemporalDataGenerator,
    base_time: datetime
) -> List[Dict[str, Any]]:
    """
    Generate a layering pattern scenario.
    
    Layering is a form of market manipulation where multiple orders are placed
    on one side of the book to create artificial pressure, while the real
    intention is to execute on the opposite side. The manipulator typically:
    1. Places multiple small orders at progressively better prices
    2. Waits for market impact
    3. Executes a larger order on the opposite side
    4. Cancels the layered orders
    
    This scenario creates a sequence of trades that mimics this pattern
    while maintaining temporal validity.
    """
    docs = []
    scenario_type = "layering"
    
    # Select a security with enough liquidity for layering
    security = random.choice(['AAPL', 'MSFT'])  # High volume securities
    
    # Choose a counterparty for the manipulation
    manipulator_cp = random.choice(generator.counterparties)
    
    # Step 1: Create multiple layer orders (typically on the sell side)
    # These create artificial selling pressure
    num_layers = random.randint(4, 6)
    
    for i in range(num_layers):
        layer_time = base_time + timedelta(seconds=30*i)
        # Each layer slightly improves the price
        layer_trade = generator.generate_trade(
            trade_date=layer_time,
            is_suspicious=True,
            scenario_type=scenario_type,
            counterparty_id=manipulator_cp["_id"],
            side="S"  # Sell side layers
        )
        # Add metadata specific to layering
        layer_trade.update({
            "layer_sequence": i+1,
            "pattern_role": "deceptive_layer"
        })
        docs.append(layer_trade)
    
    # Step 2: Execute the real (larger) trade on the opposite side
    # after the layers have created price pressure
    execution_time = base_time + timedelta(minutes=2)
    real_trade = generator.generate_trade(
        trade_date=execution_time,
        is_suspicious=True,
        scenario_type=scenario_type,
        counterparty_id=manipulator_cp["_id"],
        side="B"  # Buy side execution
    )
    # Modify quantity to be larger than the layers
    real_trade["quantity"] *= 3
    real_trade["pattern_role"] = "actual_execution"
    docs.append(real_trade)
    
    # Step 3: Cancel the layered orders
    for i in range(num_layers):
        cancel_time = execution_time + timedelta(seconds=random.randint(1, 30))
        original, correction = generator.generate_trade_correction(
            next(t for t in docs if t.get("layer_sequence") == i+1),
            cancel_time
        )
        correction["trade_status"] = "cancelled"
        docs.extend([original, correction])
    
    return docs

def generate_wash_trading_scenario(
    generator: BitemporalDataGenerator,
    base_time: datetime
) -> List[Dict[str, Any]]:
    """
    Generate a wash trading scenario.
    
    Wash trading involves creating artificial trading activity through
    self-dealing or coordinated trading between related parties. Common patterns:
    1. Same beneficial owner trading with themselves
    2. Coordinated trading between related accounts
    3. Pre-arranged trades with no genuine change in ownership
    
    This scenario creates a series of trades that demonstrate these patterns
    while maintaining realistic market behavior.
    """
    docs = []
    scenario_type = "wash_trading"
    
    # Select two related counterparties (could be same beneficial owner)
    cp_a = random.choice(generator.counterparties)
    related_cps = [
        cp for cp in generator.counterparties 
        if cp["beneficial_owner_id"] == cp_a["beneficial_owner_id"]
        and cp["_id"] != cp_a["_id"]
    ]
    cp_b = random.choice(related_cps) if related_cps else random.choice(generator.counterparties)
    
    # Generate a series of wash trades over a short period
    num_pairs = random.randint(3, 5)
    
    for i in range(num_pairs):
        # First leg of wash trade
        trade_time = base_time + timedelta(minutes=i*15)
        trade_a = generator.generate_trade(
            trade_date=trade_time,
            is_suspicious=True,
            scenario_type=scenario_type,
            counterparty_id=cp_a["_id"],
            side="B"
        )
        trade_a["pattern_role"] = "wash_buy"
        
        # Matching second leg
        trade_b = generator.generate_trade(
            trade_date=trade_time + timedelta(minutes=random.randint(1, 5)),
            is_suspicious=True,
            scenario_type=scenario_type,
            counterparty_id=cp_b["_id"],
            side="S"
        )
        # Ensure matching characteristics
        trade_b["quantity"] = trade_a["quantity"]
        trade_b["price"] = trade_a["price"]
        trade_b["pattern_role"] = "wash_sell"
        trade_b["symbol"] = trade_a["symbol"]
        
        docs.extend([trade_a, trade_b])
    
    return docs

def generate_spoofing_scenario(
    generator: BitemporalDataGenerator,
    base_time: datetime
) -> List[Dict[str, Any]]:
    """
    Generate a spoofing scenario.
    
    Spoofing involves placing large orders with no intention to execute,
    to create artificial price pressure. The pattern typically involves:
    1. Placing large orders away from the current market
    2. Executing smaller trades in the opposite direction
    3. Quickly canceling the large orders
    
    This creates a realistic spoofing pattern with proper temporal sequencing.
    """
    docs = []
    scenario_type = "spoofing"
    
    # Select a security suitable for spoofing (liquid enough to absorb large orders)
    security = random.choice(['GOOGL', 'AMZN'])  # High-value securities
    
    # Choose a sophisticated counterparty for the manipulation
    sophisticated_cps = [
        cp for cp in generator.counterparties 
        if cp["account_type"] in ['I', 'P']  # Institutional or Proprietary
    ]
    spoofer_cp = random.choice(sophisticated_cps if sophisticated_cps else generator.counterparties)
    
    # Place the spoof order (large size, away from market)
    spoof_time = base_time
    spoof_order = generator.generate_trade(
        trade_date=spoof_time,
        is_suspicious=True,
        scenario_type=scenario_type,
        counterparty_id=spoofer_cp["_id"],
        side="S"  # Typically sell side for spoofing
    )
    # Amplify the spoof order size
    spoof_order["quantity"] *= 10
    spoof_order["pattern_role"] = "spoof_order"
    spoof_order["trade_status"] = "pending"
    docs.append(spoof_order)
    
    # Execute the real trades while spoof order is active
    num_real_trades = random.randint(2, 4)
    for i in range(num_real_trades):
        trade_time = spoof_time + timedelta(seconds=random.randint(30, 120))
        real_trade = generator.generate_trade(
            trade_date=trade_time,
            is_suspicious=True,
            scenario_type=scenario_type,
            counterparty_id=spoofer_cp["_id"],
            side="B"  # Opposite side of spoof
        )
        real_trade["symbol"] = spoof_order["symbol"]
        real_trade["pattern_role"] = "actual_execution"
        docs.append(real_trade)
    
    # Cancel the spoof order
    cancel_time = spoof_time + timedelta(minutes=2)
    original, cancelled = generator.generate_trade_correction(
        spoof_order, cancel_time
    )
    cancelled["trade_status"] = "cancelled"
    docs.extend([original, cancelled])
    
    return docs

def generate_momentum_ignition_scenario(
    generator: BitemporalDataGenerator,
    base_time: datetime
) -> List[Dict[str, Any]]:
    """
    Generate a momentum ignition scenario.
    
    Momentum ignition involves creating rapid price movements to trigger
    other market participants' momentum-based trading strategies. Pattern:
    1. Initial aggressive orders to start price movement
    2. Wait for momentum traders to join
    3. Take profit in the opposite direction
    
    This creates a realistic pattern that could trigger momentum strategies.
    """
    docs = []
    scenario_type = "momentum_ignition"
    
    # Select a security susceptible to momentum trading
    security = random.choice(['AAPL', 'TSLA'])  # High-beta stocks
    
    # Choose a sophisticated counterparty
    sophisticated_cps = [
        cp for cp in generator.counterparties 
        if cp["account_type"] == 'I'  # Institutional
    ]
    manipulator_cp = random.choice(sophisticated_cps if sophisticated_cps else generator.counterparties)
    
    # Phase 1: Aggressive initial orders to start momentum
    num_initial = random.randint(3, 5)
    for i in range(num_initial):
        trade_time = base_time + timedelta(seconds=i*10)
        ignition_trade = generator.generate_trade(
            trade_date=trade_time,
            is_suspicious=True,
            scenario_type=scenario_type,
            counterparty_id=manipulator_cp["_id"],
            side="B"  # Usually buying to start upward momentum
        )
        ignition_trade["pattern_role"] = "momentum_ignition"
        ignition_trade["symbol"] = security
        docs.append(ignition_trade)
    
    # Phase 2: Wait period for momentum traders (simulated by time gap)
    
    # Phase 3: Profit taking trades
    num_profit = random.randint(2, 4)
    for i in range(num_profit):
        trade_time = base_time + timedelta(minutes=5) + timedelta(seconds=i*30)
        profit_trade = generator.generate_trade(
            trade_date=trade_time,
            is_suspicious=True,
            scenario_type=scenario_type,
            counterparty_id=manipulator_cp["_id"],
            side="S"  # Selling into the momentum
        )
        profit_trade["pattern_role"] = "profit_taking"
        profit_trade["symbol"] = security
        docs.append(profit_trade)
    
    return docs

def get_all_scenarios(
    generator: BitemporalDataGenerator,
    base_time: datetime
) -> List[Dict[str, Any]]:
    """
    Generate all manipulation scenarios with proper spacing.
    Ensures scenarios don't interfere with each other temporally.
    """
    all_docs = []
    
    # Layering scenario
    layering_docs = generate_layering_scenario(generator, base_time)
    all_docs.extend(layering_docs)
    
    # Wash trading scenario (start 30 minutes after layering)
    wash_time = base_time + timedelta(minutes=30)
    wash_docs = generate_wash_trading_scenario(generator, wash_time)
    all_docs.extend(wash_docs)
    
    # Spoofing scenario (start 1 hour after initial)
    spoof_time = base_time + timedelta(hours=1)
    spoof_docs = generate_spoofing_scenario(generator, spoof_time)
    all_docs.extend(spoof_docs)
    
    # Momentum ignition (start 2 hours after initial)
    momentum_time = base_time + timedelta(hours=2)
    momentum_docs = generate_momentum_ignition_scenario(generator, momentum_time)
    all_docs.extend(momentum_docs)
    
    return all_docs