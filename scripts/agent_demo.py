import requests

def ask(question: str):
    if "last 30 days" in question and "CAC" in question and "ROAS" in question:
        r = requests.get("http://localhost:8000/compare_30d")
        data = r.json()
        return [m for m in data if m["metric"] in ("CAC", "ROAS")]
    else:
        return {"error": "Question not recognized"}

if __name__ == "__main__":
    q = "Compare CAC and ROAS for last 30 days vs prior 30 days"
    print(ask(q))
