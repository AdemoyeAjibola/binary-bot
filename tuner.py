# tuner.py
"""
Simple per-symbol strategy-weight tuner.

It reads labeled signals from the 'signals' table (must have 'strategy_json' and 'result'),
then uses a greedy hill-climb (integer weights) to find per-strategy weights that maximize
accuracy (win rate) when predicting using weighted majority.

Usage:
    python tuner.py

You can tune parameters inside `MIN_SIGNALS_PER_SYMBOL`, `MAX_ITER`, etc.
"""

import json
import sqlite3
from typing import Dict
from copy import deepcopy

DB = "signals.db"

STRATEGIES = ["SMA","EMA","RSI","MACD","BOLLINGER","ATR","VOLUME","ROC","SUPERTREND","ICHIMOKU","STOCH","ADX"]

MIN_SIGNALS_PER_SYMBOL = 20  # only tune symbols with >= this many labeled signals
MAX_WEIGHT = 5
MIN_WEIGHT = 0
MAX_ITER = 200

def load_signals():
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row
    c = conn.cursor()
    rows = c.execute("SELECT * FROM signals WHERE result IS NOT NULL ORDER BY received_at ASC").fetchall()
    conn.close()
    out = []
    for r in rows:
        out.append(dict(r))
    return out

def predict_with_weights(strategy_json_str, weights: Dict[str,int]):
    try:
        sm = json.loads(strategy_json_str)
    except:
        return None
    score = 0
    for s,v in sm.items():
        vote = v.get("vote","NEUT").upper()
        w = weights.get(s,1)
        if vote == "CALL": score += w
        elif vote == "PUT": score -= w
    if score > 0: return "win"  # predicted CALL -> win expected where entry-calc used? we test same as actual result
    elif score < 0: return "loss"
    else: return "neutral"

def evaluate_weights(signals, weights):
    total = 0; correct = 0
    for s in signals:
        pred = predict_with_weights(s["strategy_json"], weights)
        if pred == "neutral": continue
        # mapping: pred "win" means majority CALL, but actual result might be 'win' or 'loss'
        # For evaluation, if pred == 'win' and actual result == 'win' -> correct
        # If pred == 'loss' and actual == 'loss' -> correct
        if pred == "win" and s["result"] == "win":
            correct += 1
        elif pred == "loss" and s["result"] == "loss":
            correct += 1
        total += 1
    if total == 0: return 0.0
    return correct / total

def tune_for_symbol(signals):
    # initial weights
    weights = {s:1 for s in STRATEGIES}
    best_score = evaluate_weights(signals, weights)
    improved = True
    iter_count = 0
    while improved and iter_count < MAX_ITER:
        improved = False
        iter_count += 1
        for strat in STRATEGIES:
            for delta in (-1,1):
                new_weights = deepcopy(weights)
                new_weights[strat] = max(MIN_WEIGHT, min(MAX_WEIGHT, new_weights[strat] + delta))
                score = evaluate_weights(signals, new_weights)
                if score > best_score:
                    best_score = score
                    weights = new_weights
                    improved = True
                    print(f"Improved {strat} -> {weights[strat]} score={best_score:.3f}")
    return weights, best_score

def main():
    rows = load_signals()
    # group by symbol
    groups = {}
    for r in rows:
        sym = r["pair"]
        groups.setdefault(sym, []).append(r)
    conn = sqlite3.connect(DB); c = conn.cursor()
    for sym, sigs in groups.items():
        if len(sigs) < MIN_SIGNALS_PER_SYMBOL:
            print(f"Skipping {sym}: only {len(sigs)} signals")
            continue
        print(f"Tuning for {sym} with {len(sigs)} signals...")
        best_weights, score = tune_for_symbol(sigs)
        print(f"Best for {sym}: score={score:.3f} weights={best_weights}")
        c.execute("INSERT OR REPLACE INTO strategy_weights (symbol, weights_json) VALUES (?,?)", (sym, json.dumps(best_weights)))
        conn.commit()
    conn.close()
    print("Tuning complete.")

if __name__ == "__main__":
    main()
