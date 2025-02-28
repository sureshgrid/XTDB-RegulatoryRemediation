# ************************************************************************
# Author           : Suresh Nageswaran suresh@griddynamics.com
# File Name        : xtdb_inserter.py
# Description      : Reads the JSON files for trades & counterparties
# Connects to XTDB and inserts.
#
# Revision History :
# Date            Author            Comments
# 
# ************************************************************************
# xtdb_inserter.py

import os
import sys
import asyncio
import ijson # This is for streaming json, so I can handle larger files
import aiofiles
import json
import logging
from datetime import datetime, timezone
from typing import ( 
        Any,
        AsyncGenerator,
        List,
        Dict,
        Tuple,
        Optional         
)

import psycopg as pg

from decimal import Decimal

logger = logging.getLogger(__name__)

async def prompt_user(question: str) -> str:
    """
    Asynchronously prompt user for i/p using a backgrd thread
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, question)

class CustomJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle special data types in our trading data.
    Ensures proper serialization of timestamps and decimal values for XTDB compatibility.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

class XTDBInserter:
    """
    Handles insertion of trading and counterparty data into XTDB
    with proper bitemporal tracking and relationship preservation.
    Supports test_mode for limited database insertion while allowing
    full data generation to files.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the inserter with configuration including test_mode settings.
        
        Args:
            config: Dictionary containing database connection settings,
                   execution mode configuration, and test_mode limits
        """
        self.db_config = config.get("database", {
            "host": "localhost",
            "port": 5432,
            "dbname": "xtdb",
            "user": "xtdb",
            "password": "password"
        })

        self.execution_mode = config.get("execution_mode", {}).get("mode", "full")
        self.test_mode = config.get("execution_mode", {}).get("test_mode", 0) # No limit set, default to 0
        # self.trades_file = config["output"]["trades_file"]
        # self.counterparties_file = config["output"]["counterparties_file"]
        self.trades_file = config.get("output", {}).get("trades_file", "trades_data.json")
        self.counterparties_file = config.get("output", {}).get("counterparties_file", "counterparty_data.json")
        self.batch_size = config.get("execution_mode", {}).get("batch_size", 500)
        
        logger.info(f"Opening {self.trades_file} and {self.counterparties_file} in XTDB Inserter with batch window of {self.batch_size}\n")
        self.encoder = CustomJSONEncoder()

    
    async def insert_trades(
        self,
        cur,
        trades: List[Dict[str, Any]]
    ) -> bool:        
        """
        Insert trade documents into XTDB, following XTDB's bitemporal design patterns.
        
        Args:
            cur: Database cursor
            trades: List of trade documents to insert
        """
        
        logging.info("Inside insert_trades ...creating query string for insert")
        if not trades:
            logging.info("No trades found, nothing inserted!")
            return False
        
        logger.info("Inserting trades now ...")
        # choice = await prompt_user("Ready to insert trades? [Y/N] :")
        # if choice.lower() != 'y':
        #     print("Exiting by request ...")
        #     return False
        
        success_count = 0
        error_count = 0
        
        for trade in trades:
            try:
                # Insert with all fields in a single query
                insert_query = """
                INSERT INTO trades (
                    _id, type, scenario_type, execution_timestamp,
                    symbol, price, quantity, side,
                    executing_broker_id, executing_trader_id,
                    clearing_broker_id, clearing_account,
                    beneficial_owner_id, account_type, counterparty_id,
                    trade_report_time, settlement_date, trade_status,
                    execution_venue, execution_capacity, algo_id,
                    _valid_from
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                
                values = (
                    trade["_id"],
                    trade.get("type", "trade"),
                    trade.get("scenario_type"),
                    trade.get("execution_timestamp"),
                    trade.get("symbol", "TEST"),
                    trade.get("price", 100.00),
                    trade.get("quantity", 1),
                    trade.get("side", "buy"),
                    trade.get("executing_broker_id"),
                    trade.get("executing_trader_id"),
                    trade.get("clearing_broker_id"),
                    trade.get("clearing_account"),
                    trade.get("beneficial_owner_id"),
                    trade.get("account_type"),
                    trade.get("counterparty_id"),
                    trade.get("trade_report_time"),
                    trade.get("settlement_date"),
                    trade.get("trade_status", "executed"),
                    trade.get("execution_venue"),
                    trade.get("execution_capacity"),
                    trade.get("algo_id", "NONE"),
                    trade.get("_valid_from"),  # Must be included at insertion time
                    #trade.get("_valid_to")     #  Excluded
                )
                
                logger.info(f"Inserting trade {trade['_id']} with complete bitemporal data")
                await cur.execute(insert_query, values)
                success_count += 1
                
            except Exception as e:
                logger.error(f"Failed to insert trade {trade.get('_id', 'unknown')}: {e}")
                error_count += 1
                continue
        
        total = len(trades)
        logger.info(f"Trade insertion complete: {success_count}/{total} successful, {error_count}/{total} failed")
        
        return success_count > 0


    async def insert_counterparties(
        self,
        cur,
        counterparties: List[Dict[str, Any]]    
    ) -> bool:
        """
        Insert counterparty documents into XTDB, following XTDB's bitemporal design patterns.
        
        XTDB requires _valid_from and _valid_to to be included in the initial insertion
        to support its bitemporal functionality.
        
        Args:
            cur: Database cursor (provided from live connection)
            counterparties: List of counterparty documents to insert
            
        Returns:
            bool: True if insertion was successful, False otherwise
        """
        logger.info("Inside insert_counterparties ...creating query string for insert")

        if not counterparties:
            logger.info("Counterparties dictionary object is empty! Exiting")
            return False
        
        # Prompt the user for confirmation
        logger.info("Inserting counterparty records ...")
        # choice = await prompt_user("Ready to insert counterparties? [Y/N] :")
        # if choice.lower() != 'y':
        #     print("Exiting by request...")
        #     return False
        
        success_count = 0
        error_count = 0
        
        for cp in counterparties:
            try:
                # Safely access nested settlement_instructions
                settlement_instructions = cp.get("settlement_instructions", {})
                
                # Create a single insertion with all fields
                # We must include _valid_in the initial insertion
                insert_query = """
                INSERT INTO counterparties 
                (_id, type, executing_broker_id, clearing_broker_id,
                clearing_account, correspondent_id, beneficial_owner_id,
                account_type, account_category, status, risk_rating,
                trading_limit, credit_status, margin_requirement,
                settlement_currency, settlement_method, settlement_cycle,
                _valid_from, cp_update_sequence)
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    cp["_id"],
                    cp.get("type", "counterparty"),
                    cp.get("executing_broker_id"),
                    cp.get("clearing_broker_id"),
                    cp.get("clearing_account"),
                    cp.get("correspondent_id"),
                    cp.get("beneficial_owner_id"),
                    cp.get("account_type"),
                    cp.get("account_category"),
                    cp.get("status", "active"),
                    cp.get("risk_rating"),
                    cp.get("trading_limit", 100000),
                    cp.get("credit_status"),
                    cp.get("margin_requirement"),
                    settlement_instructions.get("default_currency", "USD"),
                    settlement_instructions.get("settlement_method", "wire transfer"),
                    settlement_instructions.get("settlement_cycle", "T+2"),
                    cp.get("_valid_from"),  # Must be included at insertion time
                    # cp.get("_valid_to"),    # Excluded
                    cp.get("cp_update_sequence", 1)
                )
                
                # Process one record at a time
                logger.info(f"Inserting counterparty {cp['_id']} with complete bitemporal data {values}")
                await cur.execute(insert_query, values)
                success_count += 1
                
            except Exception as e:
                logger.error(f"Failed to insert counterparty {cp.get('_id', 'unknown')}: {e}")
                error_count += 1
                continue
        
        # Report the results
        total = len(counterparties)
        logger.info(f"Counterparty insertion complete: {success_count}/{total} successful, {error_count}/{total} failed")
        
        return success_count > 0


    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            List of query results as dictionaries
        """
        logger.info(f"The query being attempted is: {query}")
        try:
            async with await pg.AsyncConnection.connect(**self.db_config) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    results = await cur.fetchall()
                    if results:
                        columns = [desc[0] for desc in cur.description]
                        return [dict(zip(columns, row)) for row in results]
                    return []
        except Exception as e:
            print(f"Query execution error: {e}")
            raise

    async def stream_json_data(
        self,
        file_name: str
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Stream JSON data in batches using ijson in synchronous mode,
        but yield asynchronously. This avoids needing a nonexistent
        ijson.asyncio package.

        Args:
            file_name (str): Path to the JSON file (with a top-level array of objects).

        Yields:
            List[Dict[str, Any]]: Batches of parsed objects from the JSON file.
        """
        current_batch: List[Dict[str, Any]] = []
        records_processed = 0

        try:
            # Open file in binary mode for ijson
            with open(file_name, 'rb') as file:
                # ijson.items(file, 'item') streams each object in a top-level JSON array
                for item in ijson.items(file, 'item'):
                    current_batch.append(item)
                    records_processed += 1

                    # If we've reached batch_size, yield and reset
                    if len(current_batch) >= self.batch_size:
                        # Yield the current batch asynchronously
                        yield current_batch
                        current_batch = []

                    # If test_mode is set, stop after N total records
                    if self.test_mode and records_processed >= self.test_mode:
                        break

            # Yield any leftover items if we didn't hit batch_size exactly
            if current_batch:
                yield current_batch

        except Exception as e:
            logger.error(f"Error streaming JSON from {file_name}: {str(e)}")
            raise
 
    async def ingest_bitemporal_data(
        self,
        trades: Optional[List[Dict[str, Any]]] = None,
        counterparties: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Handles ingestion of counterparties and trades into XTDB with test_mode and rollback support.
        
        - If `trades` and `counterparties` are provided, inserts directly.
        - If `trades` and `counterparties` are None, reads from JSON files.
        - If `test_mode` is enabled, limits the number of records inserted.

        Args:
            trades (Optional[List[Dict[str, Any]]]): List of trade documents to insert.
            counterparties (Optional[List[Dict[str, Any]]]): List of counterparty documents to insert.

        Returns:
            Dict[str, Any]: Execution results with insertion statistics.
        """
        is_file_ingestion = trades is None and counterparties is None
        logger.info(
            f"{'Streaming from JSON files' if is_file_ingestion else 'Inserting in-memory data'} into DB"
        )

        # Determine test_mode limit
        test_mode_limit = self.test_mode if self.test_mode and self.test_mode > 0 else None

        try:
            # Connect to the database asynchronously
            async with await pg.AsyncConnection.connect(**self.db_config, autocommit=True ) as conn:
                conn.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)

                async with conn.cursor() as cur:
                    processed_cp_ids = set()

                    # FILE-BASED INGESTION
                    if is_file_ingestion:
                        # 1) Process counterparties
                        logger.info(f"Inserting counterparties from {self.counterparties_file} in batches...")

                        async for cp_batch in self.stream_json_data(self.counterparties_file):
                            # If test_mode is enabled, track CP IDs
                            if test_mode_limit:
                                processed_cp_ids.update(cp['_id'] for cp in cp_batch)

                            await self.insert_counterparties(cur, cp_batch)
                            await conn.commit()  # commit after each batch

                        # 2) Process trades
                        logger.info(f"Inserting trades from {self.trades_file} in batches...")

                        async for trade_batch in self.stream_json_data(self.trades_file):
                            # If in test mode and we have a set of valid CP IDs, filter trades
                            if test_mode_limit and processed_cp_ids:
                                trade_batch = [
                                    t for t in trade_batch
                                    if t.get('counterparty_id') in processed_cp_ids
                                ]

                            if trade_batch:
                                await self.insert_trades(cur, trade_batch)
                                await conn.commit()

                    # IN-MEMORY INGESTION
                    else:
                        logger.info("Inserting in-memory data...")

                        # Process counterparties if provided
                        if counterparties:
                            if test_mode_limit:
                                db_counterparties = counterparties[:test_mode_limit]
                                processed_cp_ids = {cp['_id'] for cp in db_counterparties}
                            else:
                                db_counterparties = counterparties

                            await self.insert_counterparties(cur, db_counterparties)

                        # Process trades if provided
                        if trades:
                            if test_mode_limit:
                                # Limit number of trades and ensure they reference valid CP IDs if we have any
                                db_trades = [
                                    tr for tr in trades[:test_mode_limit]
                                    if not processed_cp_ids or tr.get('counterparty_id') in processed_cp_ids
                                ]
                            else:
                                db_trades = trades

                            await self.insert_trades(cur, db_trades)

                        # Commit all in-memory data together
                        await conn.commit()

            # Return execution summary
            logger.info("Data ingestion completed successfully")
            return {
                "status": "success",
                "message": "Data inserted into the database",
                "mode": "file-based" if is_file_ingestion else "memory-based",
                "test_mode_limit": test_mode_limit,
                "stats": {
                    "counterparties_processed": len(processed_cp_ids) if processed_cp_ids else None
                }
            }

        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            # Attempt a rollback
            if 'conn' in locals() and conn:
                await conn.rollback()
            raise

