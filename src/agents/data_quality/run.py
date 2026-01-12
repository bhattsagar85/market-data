from agents.data_quality.data_completeness_agent import DataCompletenessAgent

SYMBOLS = ["INFY", "TCS", "RELIANCE"]

agent = DataCompletenessAgent()

for s in SYMBOLS:
    print(agent.check_daily_coverage(s))
    print(agent.check_missing_daily_days(s))
    for tf in ["1M", "5M", "15M"]:
        print(agent.check_freshness(s, tf))
    print("-" * 60)
