import requests
import json

def ask(question: str):
    """
    Very simple NL → API mapping demo.
    Currently supports only the query:
    'Compare CAC and ROAS for last 30 days vs prior 30 days'
    """
    question = question.lower()

    if "last 30 days" in question and "cac" in question and "roas" in question:
        r = requests.get("http://localhost:8000/compare_30d")
        r.raise_for_status()
        data = r.json()
        return [m for m in data if m["metric"] in ("CAC", "ROAS")]
    
    return [{"error": "Question not recognized"}]


if __name__ == "__main__":
    q = "Compare CAC and ROAS for last 30 days vs prior 30 days"
    result = ask(q)

    print("\n📊 CAC & ROAS Comparison (last 30d vs prior 30d)\n")
    for metric in result:
        if "error" in metric:
            print(metric["error"])
            continue

        print(f"Metric:      {metric['metric']}")
        print(f"  Last 30d:  {metric['last_30d']:.2f}")
        print(f"  Prev 30d:  {metric['prev_30d']:.2f}")
        print(f"  Δ Abs:     {metric['delta_abs']:.2f}")
        print(f"  Δ %:       {metric['delta_pct']:.2f}%\n")
