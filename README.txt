1. pip install -r requirements.txt
2. Adjust config.yaml
3. Run with CLI overrides

python3 main.py --config config.yaml \
                --start_date 2023-09-01 \
                --end_date 2023-09-15 \
                --no-wash_trading \
                --spoofing
4. Check o/p

5. Example queries in example_queries.sql