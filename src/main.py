# ************************************************************************
# Author           : Suresh Nageswaran suresh@griddynamics.com
# File Name        : main.py
# Description      : 
# Main execution module for XTDB synthetic data generation and analysis.
# This module orchestrates the entire process of:
#  1. Generating synthetic trading data with realistic patterns
#  2. Creating market manipulation scenarios
#  3. Inserting data into XTDB with proper bitemporal tracking
#  4. Executing temporal analysis queries
# The module uses asynchronous operations for database interactions while
# maintaining proper error handling and resource management.
# 
# Revision History :
# Date            Author            Comments
# 
# ************************************************************************
# main.py

import sys
import asyncio
import argparse
import json
import yaml
from decimal import Decimal
from datetime import datetime, timedelta
from pathlib import Path
from datetime import datetime, timezone
from typing import TypeAlias, TypeVar, NotRequired, Dict, Any, List
import logging

# Local imports
from generator import BitemporalDataGenerator
from scenarios import (
    generate_layering_scenario,
    generate_wash_trading_scenario,
    generate_spoofing_scenario,
    #generate_momentum_ignition_scenario
)
from queries import MANIPULATION_DETECTION_QUERIES
from xtdb_inserter import XTDBInserter

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)

class TradeGenerationError(Exception):
    """
    Custom exception raised when trade generation fails.
    This could be due to:
    1. No trades being generated (empty collection)
    2. Trade generation failing (None returned)
    3. Other critical trade generation failures
    """
    pass

class CounterpartyGenerationError(Exception):
    """
    Custom exception raised when counterparty generation fails.
    This could be due to:
    1. No CPs being generated (empty collection)
    2. CP generation failing (None returned)
    3. Other critical CP generation failures
    """
    pass

# Configure logging with timestamp and log level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConfigurationError(Exception):
    """Raised when configuration validation fails."""
    pass

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments for configuration.
    
    Returns:
        Namespace containing parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="XTDB Synthetic Data Generator and Analysis Tool"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to YAML configuration file"
    )
    return parser.parse_args()

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load and parse YAML configuration file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dictionary containing configuration parameters
        
    Raises:
        FileNotFoundError: If configuration file doesn't exist
        yaml.YAMLError: If configuration file is invalid
    """
    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML configuration: {e}")
        raise

def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration parameters for completeness and correctness.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        ConfigurationError: If validation fails
    """
    required_sections = {
        "date_range": ["start_date", "end_date"],
        "output": ["trades_file", "counterparties_file", "sql_file"],
        "execution_mode": ["mode"],
        "scenario_toggles": ["layering", "wash_trading", "spoofing"]
    }
    
    # Check required sections and keys
    for section, keys in required_sections.items():
        if section not in config:
            raise ConfigurationError(f"Missing required section: {section}")
        for key in keys:
            if key not in config[section]:
                raise ConfigurationError(f"Missing required key: {section}.{key}")
    
    # Validate date range
    try:
        start_date = datetime.strptime(config["date_range"]["start_date"], "%Y-%m-%d")
        end_date = datetime.strptime(config["date_range"]["end_date"], "%Y-%m-%d")
        if end_date <= start_date:
            raise ConfigurationError("End date must be after start date")
    except ValueError as e:
        raise ConfigurationError(f"Invalid date format: {e}")
    
    # Validate execution mode
    if config["execution_mode"]["mode"] not in ["local_only", "full"]:
        raise ConfigurationError("Invalid execution mode")
    
    # Validate database configuration if needed
    if config["execution_mode"]["mode"] == "full":
        if "database" not in config:
            raise ConfigurationError("Database configuration required for full mode")
        required_db_keys = ["host", "port", "dbname", "user", "password"]
        for key in required_db_keys:
            if key not in config["database"]:
                raise ConfigurationError(f"Missing database configuration: {key}")

def setup_output_directories(config: Dict[str, Any]) -> None:
    """
    Create output directories if they don't exist.
    
    Args:
        config: Configuration dictionary
    """
    for file_path in [
        config["output"]["trades_file"],
        config["output"]["counterparties_file"],
        config["output"]["sql_file"]
    ]:
        output_dir = Path(file_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

async def process_database_operations(
    trades: List[Dict[str, Any]],
    counterparties: List[Dict[str, Any]],
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process all database operations within a single async context.
    
    Args:
        trades: List of trade documents
        counterparties: List of counterparty documents
        config: Configuration dictionary
        
    Returns:
        Dictionary containing query results
        
    Raises:
        Exception: If database operations fail
    """
    try:
        inserter = XTDBInserter(config)
        test_limit = config["execution_mode"].get("test_mode")
        
        if test_limit:
            logger.info(f"Running in test mode: inserting {test_limit} records")
            trades = trades[:test_limit]
            counterparties = counterparties[:test_limit]
        
        # Single call to ingest both trades and counterparties
        await inserter.ingest_bitemporal_docs(
            trades=trades,
            counterparties=counterparties,
            config=config
        )
        
        # Execute queries within the same async context
        query_results = {}
        for query_name, query in MANIPULATION_DETECTION_QUERIES.items():
            try:
                results = await inserter.execute_query(query)
                query_results[query_name] = results
            except Exception as e:
                logger.error(f"Query execution failed for {query_name}: {e}")
                query_results[query_name] = {"error": str(e)}
        
        return query_results
        
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        raise

async def prompt_user(question: str) -> str:
    """
    Asynchronously prompt user for i/p using a backgrd thread
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, question)

async def main() -> None:
    """
    Main execution function coordinating data generation and analysis.
    
    This function:
    1. Loads and validates configuration
    2. Generates synthetic trading data
    3. Applies market manipulation scenarios
    4. Writes data to output files
    5. Optionally inserts data into XTDB and runs analysis
    """
    try:
        # Initialize configuration
        args = parse_arguments()
        config = load_config(args.config)
        validate_config(config)
        setup_output_directories(config)
        
        # Generate synthetic data
        logger.info("Initializing data generator...")
        generator = BitemporalDataGenerator(
            start_date=config["date_range"]["start_date"],
            end_date=config["date_range"]["end_date"],
            config=config
        )
        
        logger.info("Generating base dataset...")
        trades, counterparties = generator.generate_dataset()
        
        # Some basic error checks to see if the data was generated at all
        logger.info(f"Generated trades: {'NOT NULL' if trades is not None else 'NULL'}")
        
        # Error and Length checks on returned trades dataset
        if trades is None:
            logger.error("Trades collection is None")
            raise TradeGenerationError("Trade generation failed - received None instead of trade collection")
        elif len(trades) == 0:
            logger.error("Trade generation failed - received 0 trades in collection")
            raise TradeGenerationError("Trade generation failed - received 0 items in trade collection")
        else:
            # Spit out the count
            logger.info(f"Successful trade gen! Number of trades generated : {len(trades)}")

        # Same with counterparties
        logger.info(f"Generated counterparties: {'NOT NULL' if counterparties is not None else 'NULL'}")
        
        # Error and Length checks on returned CP dataset
        if counterparties is None:
            logger.error("counterparties collection is None")
            raise CounterpartyGenerationError("Counterparty generation failed - received None instead of counterparties collection")
        elif len(counterparties) == 0:
            logger.error("Counterparty generation failed - received 0 counterparties in collection")
            raise CounterpartyGenerationError("Counterparty generation failed - received 0 items in counterparties collection")
        else:
            # Spit out the count
            logger.info(f"Successful counterparty gen! Number of counterparties generated : {len(counterparties)}")


        logger.info("Phase II : Market manipulation!")
        # choice = await prompt_user("Base files generated. Start market manipulation? [Y/N] :")
        # if choice.lower() != 'y':
        #     print("Exiting by request ...")
        #     return

        # Apply market manipulation scenarios

        logger.info("Applying manipulation scenarios...")
        base_time = datetime.now(timezone.utc)
        
        if config["scenario_toggles"]["layering"]:
            logger.info("Generating layering scenario...")
            trades.extend(generate_layering_scenario(generator, base_time))
            
        if config["scenario_toggles"]["wash_trading"]:
            logger.info("Generating wash trading scenario...")
            trades.extend(
                generate_wash_trading_scenario(
                    generator,
                    base_time + timedelta(minutes=30)
                )
            )
            
        if config["scenario_toggles"]["spoofing"]:
            logger.info("Generating spoofing scenario...")
            trades.extend(
                generate_spoofing_scenario(
                    generator,
                    base_time + timedelta(hours=1)
                )
            )
        
        # Write output files
        logger.info("Writing output files...")
        for file_path, data in [
            (config["output"]["trades_file"], trades),
            (config["output"]["counterparties_file"], counterparties)
        ]:
           try:
               # JSON serialization err from not being able to serialize Decimal to JSON natively
               json_data = json.dumps(data, indent=4, cls=DecimalEncoder)
               Path (file_path).write_text(json_data, encoding='utf-8')
           except (TypeError, json.JSONDecodeError) as e:
               logger.error(f"JSON serialization error for {file_path}: {e}")
               raise
           except (FileNotFoundError, PermissionError) as e:
               logger.error(f"File writing error for {file_path}: {e}")
        
        logger.info("All files generated. Phase II complete!")
        logger.info("Phase III : XTDB Database Insert ...")
        choice = await prompt_user("Continue? [Y/N] :")
        if choice.lower() != 'y':
            print("Exiting by request ...")
            return
        
        # Handle database operations if needed
        # Here's where we insert into XTDB
        if config["execution_mode"]["mode"] == "full":
            logger.info("Starting database operations...")
            inserter = XTDBInserter(config) # init the inserter with the config
            # await inserter.ingest_bitemporal_docs() 
            # This will process the two JSONs
            await inserter.ingest_bitemporal_data()
        else:
            logger.info("Running in local mode, no database ops needed.")
        
        logger.info("Data generation and ingestion complete.")
        
    #except Exception as e:
    except (ValueError, TypeError, KeyError, TradeGenerationError, CounterpartyGenerationError) as e:
        logger.error(f"Process failed: {e}")
        raise

if __name__ == "__main__":
    # Single call to asyncio.run() for the entire application
    asyncio.run(main())
